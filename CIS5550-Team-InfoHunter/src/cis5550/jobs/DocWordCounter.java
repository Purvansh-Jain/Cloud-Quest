package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class will not only count the number of words in each document, but also
 * extract the title and description from the HTML page. It is so called mainly due to
 * backward compatibility.
 */
public class DocWordCounter {
    public static void run(FlameContext flameContext, String[] args) throws Exception {
        KVSClient kvs = flameContext.getKVS();
        kvs.persist("docWordCount");
        KVSClient.KVSIterator rowIterator = kvs.scan("cache");
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
                    draft.put("Url", url);
                    String title = document.title();
                    if (!title.isEmpty()) {
                        draft.put("Title", title);
                    }

                    long wordCount = 0;
                    StringBuilder descriptionStringBuilder = new StringBuilder();
                    for (Element element : document.select("meta[name=description]")) {
                        String content = element.attr("content");
                        if (!descriptionStringBuilder.isEmpty()) {
                            descriptionStringBuilder.append(" ");
                        }
                        descriptionStringBuilder.append(content);
                        wordCount += countWords(content);
                    }
                    if (!descriptionStringBuilder.isEmpty()) {
                        draft.put("Description", descriptionStringBuilder.toString());
                    }
                    draft.put("Priority2", String.valueOf(wordCount));

                    wordCount = 0;
                    for (Element element : document.body().select("*")) {
                        String text = element.ownText();
                        if (!text.isEmpty()) {
                            wordCount += countWords(text);
                        }
                    }
                    draft.put("Priority3", String.valueOf(wordCount));
                    buffer.add(draft);
                    long countValue = count.incrementAndGet();
                    if (countValue % 1000 == 0) {
                        System.out.println("[Doc Word Counter] Counted " + countValue + " pages");
                    }
                } finally {
                    phaser.arriveAndDeregister();
                }
            });
            if (buffer.size() >= 4096) {
                kvs.putQueuedRows("docWordCount", buffer);
            }
        }
        phaser.arriveAndAwaitAdvance();
        kvs.putQueuedRows("docWordCount", buffer);
    }

    private static long countWords(String text) {
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
        long result = 0;
        for (String word : text.toLowerCase().split("\\W+")) {
            if (word.matches("[a-z]{3,}") && !STOP_WORDS.contains(word)) {
                result++;
            }
        }
        return result;
    }
}
