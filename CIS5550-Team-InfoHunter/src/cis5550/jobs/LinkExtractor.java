package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;

import javax.swing.text.MutableAttributeSet;
import javax.swing.text.html.HTML;
import javax.swing.text.html.HTMLEditorKit;
import javax.swing.text.html.parser.ParserDelegator;
import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;

public class LinkExtractor {

    static public final String DELIMITER = "@!#";

    public static void run(FlameContext flameContext, String[] args) throws Exception {
        KVSClient kvs = flameContext.getKVS();
        kvs.persist("graph");
        ExecutorService threadPool = Executors.newFixedThreadPool(16);
        ConcurrentLinkedDeque<Row> rowBuffer = new ConcurrentLinkedDeque<>();
        Phaser phaser = new Phaser(1);
        KVSClient.KVSIterator rowIterator = kvs.scan("cache");
        while (rowIterator.hasNext()) {
            Row row = rowIterator.next();
            String url = row.get("url"), rawHtmlPage = row.get("page"), rowKey = row.key();
            if (rawHtmlPage == null) {
                continue;
            }
            phaser.register();
            threadPool.submit(
                    () -> {
                        try {
                            Row outputRow = new Row(rowKey);
                            List<String> links = extractLinks(rawHtmlPage, new URI(url));
                            outputRow.put("url", url);
                            outputRow.put("link", String.join(DELIMITER, links));
                            rowBuffer.add(outputRow);
                            if (rowBuffer.size() > 1024) {
                                kvs.putQueuedRows("graph", rowBuffer);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        } finally {
                            phaser.arriveAndDeregister();
                        }
                    }
            );
        }
        phaser.arriveAndAwaitAdvance();
        kvs.putQueuedRows("graph", rowBuffer);
    }

    public static List<String> extractLinks(String htmlPage, URI baseUri) throws IOException {
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
            String scheme = uri.getScheme();
            String path = uri.getPath();
            if (path == null) {
                return null;
            }
            String lowerCasePath = path.toLowerCase();
            if (lowerCasePath.endsWith(".jpg") || lowerCasePath.endsWith(".jpeg") || lowerCasePath.endsWith(".gif") || lowerCasePath.endsWith(".png") || lowerCasePath.endsWith(".txt")) {
                return null;
            }
            int port = uri.getPort();
            boolean shouldAddPort = port == -1, shouldRebuild = shouldAddPort || uri.getFragment() != null;
            if (scheme.equalsIgnoreCase("http")) {
                port = shouldAddPort ? 80 : port;
            } else if (scheme.equalsIgnoreCase("https")) {
                port = shouldAddPort ? 443 : port;
            } else {
                return null;
            }
            if (path == null || path.isEmpty()) {
                path = "/";
                shouldRebuild = true;
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
