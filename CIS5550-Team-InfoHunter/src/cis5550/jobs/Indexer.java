package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.kvs.Row;
import cis5550.tools.Stemmer;
import cis5550.kvs.KVSClient;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import javax.swing.text.html.HTMLEditorKit;
import javax.swing.text.html.parser.ParserDelegator;
import java.io.IOException;
import java.io.StringReader;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicLong;


public class Indexer {

    final static String MAJOR_DELIMITER = "@@!!##";
    final static String MINOR_DELIMITER = "@!#";
    final static Map<String, Row> rowBuffer = new ConcurrentHashMap<>();

    final static ExecutorService threadPool = Executors.newFixedThreadPool(16);

    public static void run(FlameContext flameContext, String[] args) throws Exception {
        System.out.println("Indexer started");
        KVSClient kvs = flameContext.getKVS();
        kvs.persist("index");
        Iterator<Row> rowIterator = kvs.scan("cache");
        Phaser phaser = new Phaser(1);
        long count = 0;
        AtomicLong specialCounter = new AtomicLong(0);
        while (rowIterator.hasNext()) {
            Row row = rowIterator.next();
            String url = row.get("url");
            String page = row.get("page");
            phaser.register();
            threadPool.submit(() -> {
                try {
                    Document document = Jsoup.parse(page, url);

                    Map<String, Integer> words = new HashMap<>();
                    extractWords(url, words);
                    extractWords(document.title(), words);
                    for (Map.Entry<String, Integer> entry : words.entrySet()) {
                        String word = entry.getKey();
                        rowBuffer.computeIfAbsent(word, k -> new Row(word)).compute("Priority1",
                                (k, v) -> {
                                    if (v == null) {
                                        return url + MINOR_DELIMITER + entry.getValue();
                                    } else {
                                        return v + MAJOR_DELIMITER + url + MINOR_DELIMITER + entry.getValue();
                                    }
                                }
                        );
                    }

                    words.clear();
                    for (Element element : document.select("meta[name=description]")) {
                        extractWords(element.attr("content"), words);
                    }
                    for (Map.Entry<String, Integer> entry : words.entrySet()) {
                        String word = entry.getKey();
                        if ("special".equals(word)) {
                            specialCounter.incrementAndGet();
                        }
                        rowBuffer.computeIfAbsent(word, k -> new Row(word)).compute("Priority2",
                                (k, v) -> {
                                    if (v == null) {
                                        return url + MINOR_DELIMITER + entry.getValue();
                                    } else {
                                        return v + MAJOR_DELIMITER + url + MINOR_DELIMITER + entry.getValue();
                                    }
                                }
                        );
                    }

                    words.clear();
                    for (Element element : document.body().select("*")) {
                        String text = element.ownText();
                        if (!text.isEmpty()) {
                            extractWords(text, words);
                        }
                    }
                    PriorityQueue<Map.Entry<String, Integer>> minHeap = new PriorityQueue<>(Comparator.comparingInt(Map.Entry::getValue));
                    for (Map.Entry<String, Integer> entry : words.entrySet()) {
                        if (entry.getValue() > 1) {
                            minHeap.add(entry);
                            if (minHeap.size() > 50) {
                                minHeap.poll();
                            }
                        }
                    }
                    for (Map.Entry<String, Integer> entry : minHeap) {
                        String word = entry.getKey();
                        rowBuffer.computeIfAbsent(word, k -> new Row(word)).compute("Priority3",
                                (k, v) -> {
                                    if (v == null) {
                                        return url + MINOR_DELIMITER + entry.getValue();
                                    } else {
                                        return v + MAJOR_DELIMITER + url + MINOR_DELIMITER + entry.getValue();
                                    }
                                }
                        );
                    }
                } catch (Exception e) {
                    System.out.println("Indexing Error: " + url);
                    e.printStackTrace();
                } finally {
                    phaser.arriveAndDeregister();
                }
            });
            if ((++count) % 8192 == 0) {
                Queue<Row> rowQueue = new ArrayDeque<>();
                Set<String> strings = rowBuffer.keySet();
                for (String key : strings) {
                    Row indexRow = rowBuffer.remove(key);
                    String priority3col = indexRow.get("Priority3");
                    if (indexRow.get("Priority0") == null && indexRow.get("Priority1") == null && indexRow.get("Priority2") == null &&
                            priority3col != null && !priority3col.contains(MAJOR_DELIMITER)
                    ) {
                        continue;
                    }
                    rowQueue.add(indexRow);
                }
                System.out.println("Flushing buffer, size: " + rowQueue.size());
                kvs.putQueuedRows("index", rowQueue, true);
                System.out.println("Flushed buffer: " + count);
            }
        }
        phaser.arriveAndAwaitAdvance();
        if (!rowBuffer.isEmpty()) {
            Queue<Row> rowQueue = new ArrayDeque<>(rowBuffer.values());
            kvs.putQueuedRows("index", rowQueue, true);
        }
        System.out.println("Indexer finished, special words: " + specialCounter.get());
    }

    private static void extractWords(String text, Map<String, Integer> output) throws Exception {
        final Set<String> STOP_WORDS = Set.of(
                "and", "are", "also", "can", "may", "might", "more", "than",
                "get", "let", "have", "but", "would", "should", "could", "must",
                "has", "had", "his", "her", "him", "she", "himself", "herself",
                "for", "how", "if", "in", "into", "is", "it",
                "no", "not", "of", "on", "or", "such", "too",
                "that", "the", "them", "their", "then", "there", "these",
                "they", "this", "which", "was", "what", "when", "where",
                "who", "why", "will", "with", "were", "you", "your",
                "http", "https", "www", "com", "org", "edu", "gov", "html", "htm"
        );
        for (String word : text.toLowerCase().split("\\W+")) {
            if (word.matches("[a-z]{3,}") && !STOP_WORDS.contains(word)) {
                Stemmer stemmer = new Stemmer();
                stemmer.add(word.toCharArray(), word.length());
                stemmer.stem();
                String stemmed = stemmer.toString();
                output.compute(stemmed, (k, v) -> v == null ? 1 : v + 1);
            }
        }
    }

    /**
     * Flame version of Indexer, can yield the result in a distributed way
     * but is slow and memory-consuming for our use case
     */
    @Deprecated
    public static void runFlame(FlameContext flameContext, String[] args) throws Exception {
        flameContext
                .fromTable("crawl", (row -> row.get("url") + "," + row.get("page")))
                .flatMapToPair((s) -> {
                    String[] split = s.split(",", 2);
                    return extractWordsFlame(split[1], split[0]);
                })
                .foldByKey("", (s1, s2) -> {
                    if (s1.isEmpty()) {
                        return s2;
                    }
                    if (s2.isEmpty()) {
                        return s1;
                    }
                    return s1 + "," + s2;
                })
                .flatMapToPair((FlamePair fp) -> {
                    String[] split = fp._2().split(",");
                    Arrays.sort(
                            split,
                            Comparator.comparingLong(
                                    (String s) -> s.codePoints().filter(ch -> ch == ' ').count()
                            ).reversed()
                    );
                    return Collections.singletonList(
                            new FlamePair(
                                    fp._1(),
                                    String.join(",", split)
                            )
                    );
                })
                .saveAsTable("index");
    }

    public static List<FlamePair> extractWordsFlame(String htmlPage, String url) throws IOException {
        Map<String, String> result = new HashMap<>();
        new ParserDelegator().parse(new StringReader(htmlPage), new HTMLEditorKit.ParserCallback() {
            @Override
            public void handleText(char[] data, int pos) {
                String[] words = new String(data).toLowerCase().split("\\W+");
                for (int i = 0; i < words.length; i++) {
                    if (!words[i].isEmpty()) {
                        final int loc = i + 1;
                        result.compute(words[i], (k, v) -> v == null ? url + ":" + loc : v + " " + loc);
                        Stemmer stemmer = new Stemmer();
                        stemmer.add(words[i].toCharArray(), words[i].length());
                        stemmer.stem();
                        String stemmed = stemmer.toString();
                        if (!words[i].equals(stemmed)) {
                            result.compute(stemmed, (k, v) -> v == null ? url + ":" + loc : v + " " + loc);
                        }
                    }
                }
            }
        }, true);
        return result.entrySet().stream().map(e -> new FlamePair(e.getKey(), e.getValue())).toList();
    }
}