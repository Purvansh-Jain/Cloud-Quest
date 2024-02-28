package cis5550.backend;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;

import java.io.IOException;
import java.util.*;
import com.google.gson.Gson;
public class SearchEngine {

    class SearchResult{
        private String url;
        private String title;
        private String description; // we can add keywords to make it bold
        public SearchResult(String url, String title, String description){
            this.url = url;
            this.title = title;
            this.description = description;

        }
    }

    private final String masterAddress;
    private final KVSClient kvs;

    private final String URL_SPLITER = "@@!!##";
    private final String COUNT_SPLITER = "@!#";
    public SearchEngine(String masterAddress) {
        this.masterAddress = masterAddress;
        kvs = new KVSClient(masterAddress);
    }

    public String search(String query){
        Map<String, Double> rankingMap = new HashMap<>();
        String[] words = query.split(" ");

        int length = words.length;
        Map<String, Integer> wordsMap = new HashMap<String, Integer>();
        for(String word : words){
            if(wordsMap.containsKey(word)){
                wordsMap.put(word, wordsMap.get(word) + 1);
            }else{
                wordsMap.put(word, 1);
            }
        }
        double weight = 1.0 / length;

       for (String word : wordsMap.keySet()){
           try{
                Map<String, Double> tfIdfMap = calculateTFIDF(word);
                ranking(rankingMap, tfIdfMap, weight * wordsMap.get(word));
           }catch (Exception e){
               e.printStackTrace();
           }
       }

       Set<Map.Entry<String, Double>> rankings = rankingMap.entrySet();
       List<Map.Entry<String, Double>> rankingList = new ArrayList<>(rankings);
       Collections.sort(rankingList, new Comparator<Map.Entry<String, Double>>() {
           @Override
           public int compare(Map.Entry<String, Double> o1, Map.Entry<String, Double> o2) {
               if (o1.getValue() > o2.getValue()) return -1;
               else if (o1.getValue() < o2.getValue()) return 1;
               else return 0;
           }
       });

       List<SearchResult> searchResults = new ArrayList<>();
       for (Map.Entry<String, Double> entry : rankingList) {
            String url = entry.getKey();
            try{
                Row urlRow = kvs.getRow("docWordCount", Hasher.hash(url));
                String title = urlRow.get("Title") != null ? urlRow.get("Title") : url;
                String description = urlRow.get("Description") != null ? urlRow.get("Description") : null;

                searchResults.add(new SearchResult(url, title, description));
            }catch(IOException e){
                e.printStackTrace();
            }
       }
       Gson gson = new Gson();
       return gson.toJson(searchResults);

    }



    public void ranking(Map<String, Double> rankingResult, Map<String, Double> tfIdfMap, double weight) throws IOException{

        // ranking compartor value = 0.8 * tfidf + 0.3 * pagerank
        for (String key : tfIdfMap.keySet()){
            Row pageRankRow = kvs.getRow("pageranks",key);
            double pagerank = Math.random() * 0.3;
            if (pageRankRow != null){
                String current = pageRankRow.get("current");
                pagerank = current  != null ? Double.parseDouble(current) : pagerank;
            }
            double ranking = (0.8 * tfIdfMap.get(key) + 0.2 * pagerank) * weight;
            rankingResult.put(key, rankingResult.getOrDefault(key, 0.0) + ranking);
        }

    }



    public Map<String, Double> calculateTFIDF(String word) throws IOException{
        Map<String, Integer> urlWordCountMap = new HashMap<String, Integer>();

        Row wordUrlsRow = kvs.getRow("index", word);
        if (wordUrlsRow == null) return new HashMap<String, Double>();
        String displayInTitle = wordUrlsRow.get("Priority1");
        String displayInDesc = wordUrlsRow.get("Priority2");
        String displayInBody = wordUrlsRow.get("Priority3");
        displayInTitle = displayInTitle == null ? "" : displayInTitle;
        displayInDesc = displayInDesc == null ? "" : displayInDesc;
        displayInBody = displayInBody == null ? "" : displayInBody;
        String allCount = displayInTitle + URL_SPLITER + displayInDesc + URL_SPLITER + displayInBody;
        String[] urlsCount = allCount.split(URL_SPLITER);
        for (String urlCount : urlsCount){
            String[] urlWordCount = urlCount.split(COUNT_SPLITER);
            if (urlWordCount.length != 2) continue;
            String url = urlWordCount[0];
            int count = 0;
            try {
                count = Integer.parseInt(urlWordCount[1]);
            }catch (Exception e){
                continue;
            }
            urlWordCountMap.put(url, urlWordCountMap.getOrDefault(url,0) + count);
        }

        int df = urlWordCountMap.size();
        int docCount = kvs.count("docWordCount"); // our current doc count
        double idf = Math.log((double)docCount / df);

        Map<String, Double> tfIdfMap = new HashMap<String, Double>();
        for (String url : urlWordCountMap.keySet()){
            Row docRow = kvs.getRow("docWordCount", Hasher.hash(url));
            int count = Integer.parseInt(docRow.get("Priority2")) + Integer.parseInt(docRow.get("Priority3"));
            double tf = (double)urlWordCountMap.get(url) / count * 1.0;
            double tfIdf = tf * idf;
            tfIdfMap.put(url, tfIdf);
        }
        return tfIdfMap;

    }















}
