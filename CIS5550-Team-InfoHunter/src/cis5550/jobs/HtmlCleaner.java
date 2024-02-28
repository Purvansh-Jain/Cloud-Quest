package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicLong;

public class HtmlCleaner {
    public static void run(FlameContext flameContext, String[] args) throws Exception {
        KVSClient kvs = flameContext.getKVS();
        kvs.persist("cache");
        KVSClient.KVSIterator rowIterator = kvs.scan("crawl");
        ExecutorService threadPool = Executors.newFixedThreadPool(16);
        Queue<Row> buffer = new ConcurrentLinkedDeque<>();
        AtomicLong count = new AtomicLong(0);
        Phaser phaser = new Phaser(1);
        while (rowIterator.hasNext()) {
            Row row = rowIterator.next();
            String url = row.get("url"), rawHtmlPage = row.get("page");
            if (rawHtmlPage == null) {
                continue;
            }
            phaser.register();
            threadPool.submit(() -> {
                try {
                    Row draft = new Row(row.key());
                    Document document = Jsoup.parse(rawHtmlPage, url);
                    document.select("script").remove();
                    draft.put("page", document.toString());
                    draft.put("url", url);
                    buffer.add(draft);
                    long countValue = count.incrementAndGet();
                    if (countValue % 1000 == 0) {
                        System.out.println("[HTML Cleaner] Cleaned " + countValue + " pages");
                    }
                } finally {
                    phaser.arriveAndDeregister();
                }
            });
            if (buffer.size() >= 1024) {
                kvs.putQueuedRows("cache", buffer);
            }
        }
        phaser.arriveAndAwaitAdvance();
        kvs.putQueuedRows("cache", buffer);
    }
}

