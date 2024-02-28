package cis5550.generic;

import cis5550.webserver.Server;

import java.lang.invoke.VarHandle;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

public class Master {
    static ConcurrentSkipListMap<String, Map.Entry<String, Long>> workers = new ConcurrentSkipListMap<>();

    static {
        new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(15000);
                    long now = System.currentTimeMillis();
                    workers.entrySet().removeIf(entry -> now - entry.getValue().getValue() >= 15000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    public static List<String> getWorkers() {
        long now = System.currentTimeMillis();
        List<Map.Entry<String, Long>> workerList = new ArrayList<>(workers.values());
        return workerList.stream().filter(entry -> now - entry.getValue() < 15000).map(Map.Entry::getKey).toList();
    }

    public static String workerTable() {
        long now = System.currentTimeMillis();
        StringBuilder tableRows = new StringBuilder();
        workers.forEach((id, entry) -> {
            if (now - entry.getValue() < 15000) {
                String key = entry.getKey();
                int colonIndex = key.lastIndexOf(":");
                String url = "http://" + key + "/";
                tableRows.append("<tr style=\"border: 1px solid;\">");
                tableRows.append("<td style=\"border: 1px solid;\">").append(id).append("</td>");
                tableRows.append("<td style=\"border: 1px solid;\">").append(key, 0, colonIndex).append("</td>");
                tableRows.append("<td style=\"border: 1px solid;\">").append(key, colonIndex + 1, key.length()).append("</td>");
                tableRows.append("<td style=\"border: 1px solid;\">").append("<a href=\"").append(url).append("\">").append(url).append("</a>").append("</td>");
                tableRows.append("</tr>");
            }
        });
        return "<table style=\"border: 1px solid;\">" +
                "<tr style=\"border: 1px solid;\">" +
                "<th style=\"border: 1px solid;\">ID</th>" +
                "<th style=\"border: 1px solid;\">IP</th>" +
                "<th style=\"border: 1px solid;\">Port</th>" +
                "<th style=\"border: 1px solid;\">Link</th>" +
                "</tr>" +
                "<tr style=\"border: 1px solid;\">" +
                tableRows +
                "</tr>" +
                "</table>";
    }

    public static void registerRoutes() {
        Server.get("/ping", (req, res) -> {
            String id = req.queryParams("id");
            String port = req.queryParams("port");
            if (id == null || port == null) {
                res.status(400, "Bad Request");
                return null;
            }
            if (!workers.containsKey(id)) {
                System.out.println("New worker: " + id);
            }
            workers.put(id, Map.entry(req.ip() + ":" + port,
                    System.currentTimeMillis()));
            res.type("text/plain");
            return getWorkerListResponseBody();
        });

        Server.get("/workers", (req, res) -> {
            res.type("text/plain");
            return getWorkerListResponseBody();
        });
    }

    private static String getWorkerListResponseBody() {
        List<String> resultList = new ArrayList<>();
        long now = System.currentTimeMillis();
        workers.forEach((id, entry) -> {
            if (now - entry.getValue() < 15000) {
                resultList.add(id + "," + entry.getKey());
            }
        });
        return resultList.size() + "\n" + String.join("\n", resultList);
    }
}
