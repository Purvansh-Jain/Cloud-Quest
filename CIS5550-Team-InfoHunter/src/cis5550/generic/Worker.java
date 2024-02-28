package cis5550.generic;

import cis5550.tools.HTTP;

import java.net.URL;
import java.util.*;

public class Worker {
    public static List<String> replicaNodes = Collections.emptyList();

    public static int port;

    public static String masterAddress;

    public static String id;

    public static void startPingThread() {
        new Thread(() -> {
            while (true) {
                try {
                    HTTP.Response response = HTTP.doRequestWithTimeout("GET", "http://" + masterAddress + "/ping?id=" + id + "&port=" + port, null, 3000);
                    if (response != null) {
                        if (response.statusCode() != 200) {
                            System.out.println("Received response " + response.statusCode() + " " + Arrays.toString(response.body()) + " for PING request from master");
                            continue;
                        }
                        List<Map.Entry<String, String>> aboveNodes = new ArrayList<>(), belowNodes = new ArrayList<>();
                        String[] strings = new String(response.body()).split("\n");
                        if (Integer.parseInt(strings[0]) == strings.length - 1) {
                            for (int i = 1; i < strings.length; i++) {
                                String[] workerInfo = strings[i].split(",");
                                if (id.compareTo(workerInfo[0]) < 0) {
                                    aboveNodes.add(Map.entry(workerInfo[0], workerInfo[1]));
                                } else if (id.compareTo(workerInfo[0]) > 0) {
                                    belowNodes.add(Map.entry(workerInfo[0], workerInfo[1]));
                                }
                            }
                        }
                        aboveNodes.sort(Map.Entry.comparingByKey());
                        if (aboveNodes.size() < 2) {
                            belowNodes.sort(Map.Entry.comparingByKey());
                            int moreCount = Math.min(belowNodes.size(), 2 - aboveNodes.size());
                            for (int i = 0; i < moreCount; i++) {
                                aboveNodes.add(belowNodes.get(i));
                            }
                        }
                        replicaNodes = aboveNodes.stream().limit(2).map(Map.Entry::getValue).toList();
                    }
                    Thread.sleep(5000);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }
}
