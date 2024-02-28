package cis5550.backend;

import cis5550.tools.Searcher;
import cis5550.tools.URLParser;
import cis5550.webserver.Server;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;


public class ClientServer {
    static SearchEngine searchEngine;

    public ClientServer(String masterAddress) throws IOException {
        searchEngine = new SearchEngine(masterAddress);
    }


    public static String loadHTML(String path) {
        String html = "";
        try {
            File file = new File(path);
            FileReader fr = new FileReader(file);
            String line = "";
            BufferedReader br = new BufferedReader(fr);
            while ((line = br.readLine()) != null) {
                html += line;
            }
            br.close();
            fr.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return html;

    }

    public static void main(String[] args) throws IOException {

        Searcher.init(args[0]);

        Server.port(80);

        Server.securePort(443);

        // Load html File from local
        String indexHTML = loadHTML("index.html");
        String searchHTML = loadHTML("searchResult.html");

        Server.get("/", (req, res) -> {
            res.type("text/html");
            return indexHTML;
        });

        Server.get("/search", (req, res) -> {
            res.type("text/html");
            return searchHTML;
        });

        Server.get("/cache/:url", (req, res) -> {
            res.type("text/html");
            String url = URLParser.decodeNullableURLParam(req.params("url"));
            String cache = Searcher.getCache(url);
            if (cache != null) {
                return cache;
            }
            res.status(404, "Not Found");
            return null;
        });

        Server.get("/api/search", (req, res) -> {
            res.type("application/json");
            String query = URLParser.decodeNullableURLParam(req.queryParams("q"));
            String upperBound = URLParser.decodeNullableURLParam(req.queryParams("max"));
            String size = URLParser.decodeNullableURLParam(req.queryParams("limit"));
            int limit = 10;
            double max = Double.POSITIVE_INFINITY;
            if (upperBound != null && !upperBound.equals("")) {
                System.out.println("max: " + upperBound);
                max = Double.parseDouble(upperBound);
            }
            if (size != null && !size.equals("")) {
                System.out.println("size: " + size);
                limit = Integer.parseInt(size);
            }
            System.out.println("query: " + query);
            String result = Searcher.searchAPI(query, max, limit);
            return result;
        });

    }


}
