package cis5550.jobs;

import cis5550.flame.*;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;

import javax.swing.text.MutableAttributeSet;
import javax.swing.text.html.HTML;
import javax.swing.text.html.HTMLEditorKit;
import javax.swing.text.html.parser.ParserDelegator;
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipException;


public class Crawler {
    // every loop should delete last loop's rdd in kvs
    // read from log, next loop to crawl
    // if log is empty, start from seed url
    static final String DELIMITER = "@!#";

    // blacklist for URLs
    // no need to feed them as parameters since I assume they do not change frequently
    static final List<String> blacklist = new ArrayList<>(List.of());

    static int MAX_DEPTH = 15;

    public static void run(FlameContext flameContext, String[] args) throws Exception {
        if (args == null || args.length == 0) {
            flameContext.output("One seed url is required");
            return;
        }
        KVSClient kvs = flameContext.getKVS();

        kvs.persist("crawl");
        kvs.persist("host");
        kvs.persist("pages");
        kvs.persist("currentQueue"); // for future restart, check worker/put route whether it will replace table content

        String nextRound;
        int count = 0;
        FlameRDD urlQueue;
        Iterator<Row> rowItr = kvs.scan("currentQueue");
        if (rowItr == null || !rowItr.hasNext()) {
            urlQueue = flameContext.parallelize(
                    Arrays.stream(args).map(url -> {
                        try {
                            return formatUri(new URI(url)) + DELIMITER + "0";
                        } catch (URISyntaxException e) {
                            throw new RuntimeException(e);
                        }
                    }).toList(),
                    "currentQueue");
        } else {
            urlQueue = new FlameRDDImpl("currentQueue");
        }

        String kvsMasterAddress = kvs.getMaster();

        // for the urlQueue,I think we could use PairRDD here to store the url and its depth, instead of just RDD of url
        // I implement the count function in FlamePairRDDImpl, but I just use the collect size as the count
        while (urlQueue.count() > 0) {
            count += 1; // record the loop times
            nextRound = "nextRound" + count;

            urlQueue = urlQueue.flatMapRandom((String urlAndDepth) -> {
                KVSClient kvsClient = new KVSClient(kvsMasterAddress);
                String[] splits = urlAndDepth.split(DELIMITER);
                String urlString = splits[0];
                String depth = splits[1];
                String newDepth = String.valueOf(Integer.parseInt(depth) + 1);
                String urlHash = Hasher.hash(urlString), contentType;
                URI baseUri = new URI(urlString);
                String host = baseUri.getHost();
                try {
                    if (blacklist.stream().anyMatch(urlString::contains)) {
                        return Collections.emptyList();
                    }
                    if (kvsClient.existsRow("crawl", urlHash)) {
                        return Collections.emptyList();
                    }
                    if (Integer.parseInt(depth) > MAX_DEPTH) {
                        return Collections.emptyList();
                    }

                    Row hostRow = kvsClient.getRow("host", host);
                    if (hostRow == null) {
                        System.out.println("[Crawler] Fetching robots.txt for host: " + host);
                        URI robotsTxtUri = new URI(baseUri.getScheme(), baseUri.getUserInfo(), host, baseUri.getPort(),
                                "/robots.txt", null, null);
                        String robotsTxtUriString = robotsTxtUri.toString();
                        HttpURLConnection connection = newCrawlerConnection(new URL(robotsTxtUriString), "GET");
                        hostRow = new Row(host);
                        if (connection.getResponseCode() == HttpURLConnection.HTTP_OK) {
                            hostRow.put("robots", readResponseBody(connection, robotsTxtUriString));
                            System.out.println("[Crawler] Successfully fetched robots.txt for host: " + host);
                        } else {
                            System.out.println("[Crawler] Failed to fetch robots.txt for host: " + host + ", response code: " + connection.getResponseCode());
                        }
                        hostRow.put("depth", "0");
                        kvsClient.putRow("host", hostRow);
                    }
                    long delay = getDelayFromRobotsTxt(hostRow.get("robots"), baseUri.getPath());
                    if (delay == Long.MIN_VALUE) {
                        return Collections.emptyList();
                    }
                    if (delay > 5000) {
                        System.out.println("[Crawler] Host: " + host + " has a delay of " + delay + "ms");
                    }
                    long waitingStartTime = System.currentTimeMillis();
                    do {
                        byte[] lastCrawlTimeBytes = kvsClient.get("host", host, "lastCrawlTime");
                        if (lastCrawlTimeBytes == null) {
                            break;
                        }
                        String lastCrawlTime = new String(lastCrawlTimeBytes);
                        long lastCrawlTimeLong = Long.parseLong(lastCrawlTime);
                        long now = System.currentTimeMillis();
                        if (now - lastCrawlTimeLong <= delay) {
                            if (now - waitingStartTime > 5000) {
                                System.out.println("[Crawler] Current thread has waited " + (now - waitingStartTime) + "ms for host: " + host);
                                if (now - waitingStartTime > 10000) {
                                    System.out.println("[Crawler] Task is deferred to next cycle after waiting for 30s, url: " + urlString);
                                    return Collections.singletonList(urlString + DELIMITER + depth);
                                }
                            }
                            long duration = delay - (now - lastCrawlTimeLong) + ThreadLocalRandom.current().nextInt(0, 1000);
                            Thread.sleep(duration);
                        } else {
                            break;
                        }
                    } while (true);
                    URL url = new URL(urlString);
                    int headResponseCode;
                    {
                        kvsClient.put("host", host, "lastCrawlTime", String.valueOf(System.currentTimeMillis()));
                        HttpURLConnection connection = newCrawlerConnection(url, "HEAD");
                        connection.setInstanceFollowRedirects(false);
                        Row row = new Row(urlHash);
                        headResponseCode = connection.getResponseCode();
                        System.out.println("[Crawler] Fetched HEAD for url: " + urlString + ", response code: " + headResponseCode);
                        row.put("responseCode", String.valueOf(headResponseCode));
                        row.put("url", urlString);
                        contentType = connection.getContentType();
                        if (contentType != null) {
                            row.put("contentType", contentType);
                        }
                        int contentLength = connection.getContentLength();
                        if (contentLength != -1) {
                            row.put("length", String.valueOf(contentLength));
                        }
                        String language = connection.getHeaderField("content-language");
                        if (language != null) {
                            language = language.toLowerCase(Locale.ROOT);
                            row.put("language", language);
                        }
                        kvsClient.putRow("crawl", row);
                        if (language != null && !language.contains("en")) {
                            return Collections.emptyList();
                        }
                        if (headResponseCode == 301 ||
                                headResponseCode == 302 ||
                                headResponseCode == 303 ||
                                headResponseCode == 307 ||
                                headResponseCode == 308) {
                            String location = connection.getHeaderField("Location");
                            if (location != null) {
                                URI resolved;
                                try {
                                    resolved = baseUri.resolve(location);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                    System.out.println("[Crawler] Redirection failed, url: " + urlString + ", location" + location + "\n");
                                    return Collections.emptyList();
                                }
                                String newUri = formatUri(resolved);
                                if (newUri != null) {
                                    String nextUrlDepth = newUri + DELIMITER + depth;
                                    return Collections.singletonList(nextUrlDepth);
                                }
                            }
                        }
                    }

                    if (headResponseCode == HttpURLConnection.HTTP_BAD_METHOD || (  // HEAD request not supported, continue anyway
                            headResponseCode == HttpURLConnection.HTTP_OK &&
                                    contentType != null && contentType.startsWith("text/html")
                    )) {
                        HttpURLConnection connection = newCrawlerConnection(url, "GET");
                        int responseCode = connection.getResponseCode();
                        kvsClient.put("host", host, "lastCrawlTime", String.valueOf(System.currentTimeMillis()));
                        if (responseCode == HttpURLConnection.HTTP_OK) {
                            byte[] responseBodyBytes = readResponseBody(connection, urlString);
                            String responseBodyText = new String(responseBodyBytes);
                            String pageHash = Hasher.hash(responseBodyText);
                            Row pageRow = kvsClient.getRow("pages", pageHash);
                            if (pageRow != null) {
                                for (String prevUrlHash : pageRow.columns()) {
                                    String prevUrl = pageRow.get(prevUrlHash);
                                    if (Arrays.equals(
                                            responseBodyBytes,
                                            kvsClient.get("crawl", prevUrlHash, "page")
                                    )) {
                                        kvsClient.put("crawl", urlHash, "canonicalURL", prevUrl);
                                        return Collections.emptyList();
                                    }
                                }
                            }
                            if (headResponseCode == HttpURLConnection.HTTP_BAD_METHOD) {
                                kvsClient.put("crawl", urlHash, "responseCode", String.valueOf(responseCode));
                            }
                            kvsClient.put("crawl", urlHash, "page", responseBodyBytes);
                            kvsClient.put("pages", pageHash, urlHash, urlString);
                            return extractLinks(responseBodyText, baseUri, blacklist)
                                    .stream()
                                    .filter(link -> {
                                        if (link.length() > 300) {
                                            System.out.println("[Crawler] Skipped long link: " + link);
                                            return false;
                                        }
                                        try {
                                            return !kvsClient.existsRow("crawl", Hasher.hash(link));
                                        } catch (IOException e) {
                                            e.printStackTrace();
                                            System.out.println("[Crawler] Failed to check existence of url: " + link);
                                            return true;
                                        }
                                    })
                                    .map(link -> link + DELIMITER + newDepth)
                                    .toList();
                        } else {
                            kvsClient.put("crawl", urlHash, "responseCode", String.valueOf(responseCode));
                        }
                    }
                } catch (SocketTimeoutException | UnknownHostException e) {
                    e.printStackTrace();
                    String blacklistTarget = host;
                    if (blacklistTarget.startsWith("www.")) {
                        blacklistTarget = blacklistTarget.substring("www.".length());
                    }
                    blacklist.add(blacklistTarget);
                    System.out.println("[Crawler] Blacklisted unreachable host: " + blacklistTarget);
                }
                return Collections.emptyList();
            }, nextRound);
            kvs.delete("currentQueue"); // delete lastRound urlQueue PairRDD
            urlQueue.saveAsTable("currentQueue");
        }
    }

    private static HttpURLConnection newCrawlerConnection(URL url, String method) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod(method);
        connection.setRequestProperty("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9");
        connection.setRequestProperty("Accept-Encoding", "gzip");
        connection.setRequestProperty("Accept-Language", "en-US,en;q=0.9");
//        connection.setRequestProperty("User-Agent", "cis5550-crawler");
        connection.setRequestProperty("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36 Edg/112.0.1722.48");
        connection.setConnectTimeout(20000);
        connection.setReadTimeout(20000);
        return connection;
    }

    private static byte[] readResponseBody(HttpURLConnection connection, String urlString) throws IOException {
        String encoding = connection.getContentEncoding();
        try (InputStream is = connection.getInputStream()) {
            try {
                if ("gzip".equalsIgnoreCase(encoding)) {
                    return new GZIPInputStream(is).readAllBytes();
                }
            } catch (ZipException e) {
                encoding = null;
                e.printStackTrace();
                System.out.println("[Crawler] Gzip failed - fall back to read as plain text, URL: " + urlString);
            }
            if (encoding == null) {
                return is.readAllBytes();
            }
        }
        throw new IOException("Unknown encoding: " + encoding + " for URL: " + urlString);
    }

    private static long getDelayFromRobotsTxt(String robotsTxt, String path) {
        long delay = 400;
        if (robotsTxt == null || robotsTxt.isEmpty()) {
            return delay;
        }
        String[] lines = robotsTxt.split("\r\n|\r|\n");
        String allowedTarget = null, disallowedTarget = null;
        boolean shouldApplyRuleToThisCrawler = false;
        for (String line : lines) {
            String lineLowerCase = line.toLowerCase();
            if (lineLowerCase.startsWith("user-agent:")) {
                String target = line.substring("user-agent:".length()).trim();
                shouldApplyRuleToThisCrawler = "*".equals(target);
            }
            if (!shouldApplyRuleToThisCrawler) {
                continue;
            }
            if (lineLowerCase.startsWith("allow:")) {
                String target = line.substring("allow:".length()).trim();
                if (path.startsWith(target) && (allowedTarget == null || allowedTarget.length() < target.length())) {
                    allowedTarget = target;
                }
                continue;
            }
            if (lineLowerCase.startsWith("disallow:")) {
                String target = line.substring("disallow:".length()).trim();
                if (path.startsWith(target) && (disallowedTarget == null || disallowedTarget.length() < target.length())) {
                    disallowedTarget = target;
                }
                continue;
            }
            if (lineLowerCase.startsWith("crawl-delay:")) {
                String target = line.substring("crawl-delay:".length()).trim();
                delay = (long) (Double.parseDouble(target) * 1000);
            }
        }
        if (disallowedTarget != null && (allowedTarget == null || allowedTarget.length() < disallowedTarget.length())) {
            return Long.MIN_VALUE;
        }
        return delay;
    }

    public static List<String> extractLinks(String htmlPage, URI baseUri, List<String> blacklist) throws IOException {
        Set<String> result = new HashSet<>();
        new ParserDelegator().parse(new StringReader(htmlPage), new HTMLEditorKit.ParserCallback() {
            @Override
            public void handleStartTag(HTML.Tag tag, MutableAttributeSet mutableAttributeSet, int pos) {
                if (tag == HTML.Tag.A) {
                    String href = (String) mutableAttributeSet.getAttribute(HTML.Attribute.HREF);
                    if (href != null) {
                        if (href.startsWith("#")) {
                            return;
                        }
                        URI resolved;
                        try {
                            resolved = baseUri.resolve(href);
                        } catch (Exception e) {
                            return;
                        }
                        String newUri = formatUri(resolved);
                        if (newUri != null) {
                            if (blacklist != null) {
                                for (String pattern : blacklist) {
                                    if (newUri.contains(pattern)) {
                                        System.out.println("Skipped blacklisted URL: " + newUri);
                                        return;
                                    }
                                }
                            }
                            result.add(newUri);
                        }
                    }
                }
            }
        }, true);
        return new ArrayList<>(result);
    }

    public static String formatUri(URI uri) {
        try {
            uri = uri.parseServerAuthority();
        } catch (URISyntaxException e) {
            System.out.println("URISyntaxException: " + e.getMessage());
        }
        {
            String scheme = uri.getScheme(), path = uri.getPath();
            boolean shouldRebuild = false;
            if (path != null && !path.isEmpty()) {
                String lowerCasePath = path.toLowerCase();
                if (lowerCasePath.endsWith(".jpg") ||
                        lowerCasePath.endsWith(".jpeg") ||
                        lowerCasePath.endsWith(".gif") ||
                        lowerCasePath.endsWith(".png") ||
                        lowerCasePath.endsWith(".txt")) {
                    return null;
                }
            } else {
                path = "/";
                shouldRebuild = true;
            }
            int port = uri.getPort();
            boolean shouldAddPort = port == -1;
            if (shouldAddPort || uri.getFragment() != null) {
                shouldRebuild = true;
            }
            if (scheme.equalsIgnoreCase("http")) {
                port = shouldAddPort ? 80 : port;
            } else if (scheme.equalsIgnoreCase("https")) {
                port = shouldAddPort ? 443 : port;
            } else {
                return null;
            }
            if (shouldRebuild) {
                try {
                    uri = new URI(scheme, uri.getUserInfo(), uri.getHost(), port, path, uri.getQuery(), null);
                } catch (URISyntaxException e) {
                    System.out.println("URISyntaxException: " + e.getMessage());
                }
            }
        }
        return uri.toString();
    }
}
