package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Stemmer;

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


public class PageRank {
    final static double DECAY_FACTOR = 0.85;

    public static void run(FlameContext flameContext, String[] args) throws Exception {
        final Map<String, Map.Entry<Double, Double>> pageRank = new ConcurrentHashMap<>(65536);
        KVSClient kvs = flameContext.getKVS();
        kvs.persist("rank");
        ExecutorService threadPool = Executors.newFixedThreadPool(16);
        {
            long counter = 0;
            KVSClient.KVSIterator rowIterator = kvs.scan("graph");
            while (rowIterator.hasNext()) {
                Row row = rowIterator.next();
                String url = row.get("url");
                pageRank.put(url, new AbstractMap.SimpleEntry<>(1.0, 0.0));
                if (++counter % 4096 == 0) {
                    System.out.println("Loaded " + counter + " links");
                }
            }
        }
        Phaser phaser = new Phaser(1);
        for (int i = 0; i < 10; i++) {
            KVSClient.KVSIterator rowIterator = kvs.scan("graph");
            AtomicLong counter = new AtomicLong(0);
            while (rowIterator.hasNext()) {
                Row row = rowIterator.next();
                String url = row.get("url");
                String linkString = row.get("link");
                if (linkString == null) {
                    continue;
                }
                if (phaser.getUnarrivedParties() > 4096) {
                    Thread.sleep(100);
                }
                phaser.register();
                int finalI = i;
                threadPool.submit(() -> {
                    try {
                        Map.Entry<Double, Double> entry = pageRank.get(url);
                        double currentRank = entry != null ? entry.getKey() : 1.0;
                        String[] linkArray = linkString.split(LinkExtractor.DELIMITER);
                        double contribution = currentRank / linkArray.length;
                        for (String link : linkArray) {
                            if (pageRank.containsKey(link)) {
                                pageRank.compute(link, (k, v) -> new AbstractMap.SimpleEntry<>(v.getKey(), v.getValue() + contribution));
                            }
                        }
                        long counterValue = counter.incrementAndGet();
                        if (counterValue % 4096 == 0) {
                            System.out.println("Processed " + counterValue + " links in iteration " + finalI);
                        }
                    } finally {
                        phaser.arriveAndDeregister();
                    }
                });
            }
            phaser.arriveAndAwaitAdvance();
            for (String key : pageRank.keySet()) {
                Map.Entry<Double, Double> entry = pageRank.get(key);
                Double rank = entry.getValue();
                pageRank.put(key, new AbstractMap.SimpleEntry<>(rank * DECAY_FACTOR + (1 - DECAY_FACTOR), 0.0));
            }
            System.out.println("Iteration " + i + " finished");
        }
        System.out.println("PageRank finished, writing to KVS");
        Queue<Row> rowBuffer = new ArrayDeque<>();
        for (String key : pageRank.keySet()) {
            Map.Entry<Double, Double> entry = pageRank.get(key);
            Double rank = entry.getKey();
            Row row = new Row(key);
            row.put("rank", rank.toString());
            rowBuffer.add(row);
            if (rowBuffer.size() > 1024) {
                kvs.putQueuedRows("rank", rowBuffer);
            }
        }
        kvs.putQueuedRows("rank", rowBuffer);
    }

    /**
     * Flame version of PageRank, can yield the result in a distributed way
     * but is slow and memory-consuming for our use case
     */
    @Deprecated
    public static void runFlame(FlameContext flameContext, String[] args) throws Exception {
        flameContext
                .fromTable("cache", (row -> row.get("url") + "," + row.get("page")))
                .flatMapToPair((s) -> {
                    String[] split = s.split(",", 2);
                    return extractWords(split[1], split[0]);
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

    public static List<FlamePair> extractWords(String htmlPage, String url) throws IOException {
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


