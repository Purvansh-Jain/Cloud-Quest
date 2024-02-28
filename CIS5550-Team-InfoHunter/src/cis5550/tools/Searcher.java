package cis5550.tools;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;
import cis5550.tools.Stemmer;
import com.google.gson.Gson;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Searcher {
    static public class SearchResultEntry {
        String url;
        String title;
        String description;
        double score;

        public SearchResultEntry(String url, String title, String description, double score) {
            this.url = url;
            this.title = title;
            this.description = description;
            this.score = score;
        }
    }

    static final ConcurrentHashMap<String, Row> globalDocInfoCache = new ConcurrentHashMap<>();
    static final String MAJOR_DELIMITER = "@@!!##";
    static final String MINOR_DELIMITER = "@!#";

    static KVSClient kvs;

    public static void init(String address) {
        if (kvs == null) {
            kvs = new KVSClient(address);
        }
    }

    public static void main(String[] args) throws Exception {
        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.print("Enter words to search: ");
            String line = scanner.nextLine();
            List<SearchResultEntry> search = search(line);
            for (SearchResultEntry searchResultEntry : search) {
                System.out.println(searchResultEntry.url + " " + searchResultEntry.score);
            }
        }
    }

    private static List<SearchResultEntry> search(String line) throws Exception {
        return search(line, Double.POSITIVE_INFINITY, 10);
    }

    private static List<SearchResultEntry> search(String line, double scoreUpperBound, int resultSizeLimit) throws Exception {
        System.out.println("Searching for: " + line + " with score upper bound " + scoreUpperBound + " and result size limit " + resultSizeLimit);
        List<String> words = Arrays.stream(line.split("\\W+")).map(
                word -> {
                    Stemmer stemmer = new Stemmer();
                    stemmer.add(word.toCharArray(), word.length());
                    stemmer.stem();
                    return stemmer.toString();
                }
        ).toList();
        List<Row> indexRows = kvs.getRows("index", words);
        List<String> priorities = List.of(
                "Priority1",
                "Priority2",
                "Priority3"
        );
        Map<String, Row> localDocInfoCache = new HashMap<>();
        Map<String, Integer> wordCount = new HashMap<>(), neededDocs = new HashMap<>();
        for (Row indexRow : indexRows) {
            if(indexRow == null) {
                continue;
            }
            String word = indexRow.key();
            int priorityMultiplier = 64;
            for (String priority : priorities) {
                String urlCountConcatString = indexRow.get(priority);
                if (urlCountConcatString == null) {
                    continue;
                }
                String[] split = urlCountConcatString.split(MAJOR_DELIMITER);
                for (String urlCount : split) {
                    String[] urlCountSplit = urlCount.split(MINOR_DELIMITER, 2);
                    String url = urlCountSplit[0];
                    int count = Integer.parseInt(urlCountSplit[1]);
                    Row globalCachedDocInfo = globalDocInfoCache.get(url);
                    if (globalCachedDocInfo != null) {
                        localDocInfoCache.put(url, globalCachedDocInfo);
                    } else {
                        int weightInc = priorityMultiplier * count;
                        neededDocs.compute(url, (k, v) -> v == null ? weightInc : v + weightInc);
                    }
                }
                wordCount.compute(word, (k, v) -> v == null ? split.length : v + split.length);
                priorityMultiplier /= 4;
            }
        }
        System.out.println("Need " + neededDocs.size() + " documents");
        List<String> docsToFetch = neededDocs
                .entrySet()
                .stream()
                .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                .limit(5000)
                .map(entry -> Hasher.hash(entry.getKey()))
                .toList();
        System.out.println("Fetching " + docsToFetch.size() + " documents");
        List<Row> docInfoRows = kvs.getRows("docWordCount", docsToFetch);
        for (Row docInfoRow : docInfoRows) {
            String url = docInfoRow.get("Url");
            localDocInfoCache.put(url, docInfoRow);
            globalDocInfoCache.put(url, docInfoRow);
        }
        System.out.println("Available: " + localDocInfoCache.size() + " documents");

        HashMap<String, Double> scores = new HashMap<>();
        for (Row indexRow : indexRows) {
            String word = indexRow.key();
            HashMap<String, Double> urlWeights = new HashMap<>();
            for (String priority : priorities) {
                String urlCountConcatString = indexRow.get(priority);
                if (urlCountConcatString == null) {
                    continue;
                }
                for (String urlCount : urlCountConcatString.split(MAJOR_DELIMITER)) {
                    String[] urlCountSplit = urlCount.split(MINOR_DELIMITER, 2);
                    String url = urlCountSplit[0];
                    Row docWordCountRow = localDocInfoCache.get(url);
                    if (docWordCountRow == null) {
                        continue;
                    }
                    String totalString = docWordCountRow.get(priority);
                    int total = totalString == null ? 0 : Integer.parseInt(totalString);
                    int count = Integer.parseInt(urlCountSplit[1]);
                    urlWeights.compute(url, (k, v) -> {
                        if (v == null) {
                            return 16 + count / Math.sqrt(1 + total);
                        } else {
                            return v + count / Math.sqrt(1 + total);
                        }
                    });
                }
            }
            for (Map.Entry<String, Double> urlCountEntry : urlWeights.entrySet()) {
                String url = urlCountEntry.getKey();
                double count = urlCountEntry.getValue();
                scores.compute(url, (k, v) -> {
                    if (v == null) {
                        return Math.pow(count, 8 / (1 + Math.log(wordCount.get(word))));
                    } else {
                        return v * Math.pow(count, 8 / (1 + Math.log(wordCount.get(word))));
                    }
                });
            }
        }
        return scores
                .entrySet()
                .stream()
                .filter(entry -> entry.getValue().compareTo(scoreUpperBound) < 0)
                .sorted(Map.Entry.<String, Double>comparingByValue().reversed())
                .limit(resultSizeLimit)
                .map(entry -> {
                    Row docInfo = localDocInfoCache.get(entry.getKey());
                    return new SearchResultEntry(
                            entry.getKey(),
                            docInfo.get("Title"),
                            docInfo.get("Description"),
                            entry.getValue()
                    );
                })
                .toList();
    }


    public static String searchAPI(String line, double scoreUpperBound, int resultSizeLimit) throws Exception {
        List<SearchResultEntry> result = search(line, scoreUpperBound, resultSizeLimit);
        Gson gson = new Gson();
        return gson.toJson(result);
    }

    public static String getCache(String url) throws IOException {
        System.out.println("Getting cache for " + url);
        byte[] bytes = kvs.get("cache", Hasher.hash(url), "page");
        if (bytes == null) {
            return null;
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }

}
