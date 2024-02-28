package cis5550.kvs;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.net.*;
import java.io.*;

import cis5550.tools.HTTP;
import cis5550.tools.StringUtil;

public class KVSClient implements KVS, Serializable {

    String master;

    static class WorkerEntry implements Comparable<WorkerEntry>, Serializable {
        String address;
        String id;

        WorkerEntry(String addressArg, String idArg) {
            address = addressArg;
            id = idArg;
        }

        public int compareTo(WorkerEntry e) {
            return id.compareTo(e.id);
        }
    }

    Vector<WorkerEntry> workers;
    boolean haveWorkers;

    public int numWorkers() throws IOException {
        if (!haveWorkers) {
            downloadWorkers();
        }
        return workers.size();
    }

    public static String getVersion() {
        return "v1.3 Oct 28 2022";
    }

    public String getMaster() {
        return master;
    }

    public String getWorkerAddress(int idx) throws IOException {
        if (!haveWorkers) {
            downloadWorkers();
        }
        return workers.elementAt(idx).address;
    }

    public String getWorkerID(int idx) throws IOException {
        if (!haveWorkers) {
            downloadWorkers();
        }
        return workers.elementAt(idx).id;
    }

    public class KVSIterator implements Iterator<Row> {
        InputStream in;
        boolean atEnd;
        Row nextRow;
        int currentRangeIndex;
        String endRowExclusive;
        String startRow;
        String tableName;
        Vector<String> ranges;

        KVSIterator(String tableNameArg, String startRowArg, String endRowExclusiveArg) throws IOException {
            in = null;
            currentRangeIndex = 0;
            atEnd = false;
            endRowExclusive = endRowExclusiveArg;
            tableName = tableNameArg;
            startRow = startRowArg;
            ranges = new Vector<>();
            for (int i = 0; i < numWorkers(); i++) {
                for (Map.Entry<String, String> workRange : resolveWorkRange(startRowArg, endRowExclusiveArg, i)) {
                    ranges.add(getURL(tableNameArg, i, workRange.getKey(), workRange.getValue()));
                }
            }
            System.out.println("[KVSClient] Ranges: " + ranges);
            openConnectionAndFill();
        }

        private List<Map.Entry<String, String>> resolveWorkRange(String startRowArg, String endRowExclusiveArg, int workerIndex) throws IOException {
            List<Map.Entry<String, String>> inputSimpleRanges = startRowArg == null || endRowExclusiveArg == null || startRowArg.compareTo(endRowExclusiveArg) < 0 ?
                    Collections.singletonList(new AbstractMap.SimpleImmutableEntry<>(startRowArg, endRowExclusiveArg)) :
                    List.of(
                            new AbstractMap.SimpleImmutableEntry<>(null, endRowExclusiveArg),
                            new AbstractMap.SimpleImmutableEntry<>(startRowArg, null)
                    );
            List<Map.Entry<String, String>> workerSimpleRanges = workerIndex < numWorkers() - 1 ?
                    Collections.singletonList(new AbstractMap.SimpleImmutableEntry<>(getWorkerID(workerIndex), getWorkerID(workerIndex + 1))) :
                    List.of(
                            new AbstractMap.SimpleImmutableEntry<>(null, getWorkerID(0)),
                            new AbstractMap.SimpleImmutableEntry<>(getWorkerID(workerIndex), null)
                    );
            List<Map.Entry<String, String>> intersections = new LinkedList<>();
            for (Map.Entry<String, String> inputSimpleRange : inputSimpleRanges) {
                for (Map.Entry<String, String> workerSimpleRange : workerSimpleRanges) {
                    Map.Entry<String, String> intersection = StringUtil.intersectSimpleRanges(inputSimpleRange, workerSimpleRange);
                    if (intersection != null) {
                        intersections.add(intersection);
                    }
                }
            }
            if (intersections.size() > 2) {
                System.out.println("[KVS Client] Resolved to more than 2 ranges: " + intersections);
            }
            if (intersections.size() == 2 && intersections.get(0).getKey() == null && intersections.get(1).getValue() == null) {
                return Collections.singletonList(new AbstractMap.SimpleImmutableEntry<>(intersections.get(1).getKey(), intersections.get(0).getValue()));
            }
            return intersections;
        }

        protected String getURL(String tableNameArg, int workerIndexArg, String startRowArg, String endRowExclusiveArg) throws IOException {
            String params = "";
            if (startRowArg != null) {
                params = "startRow=" + startRowArg;
            }
            if (endRowExclusiveArg != null) {
                params = (params.equals("") ? "" : (params + "&")) + "endRowExclusive=" + endRowExclusiveArg;
            }
            return "http://" + getWorkerAddress(workerIndexArg) + "/data/" + URLDecoder.decode(tableNameArg, StandardCharsets.UTF_8) + (params.equals("") ? "" : "?" + params);
        }

        void openConnectionAndFill() {
            try {
                if (in != null) {
                    in.close();
                    in = null;
                }

                if (atEnd) {
                    return;
                }

                while (true) {
                    if (currentRangeIndex >= ranges.size()) {
                        atEnd = true;
                        return;
                    }

                    try {
                        URL url = new URL(ranges.elementAt(currentRangeIndex));
                        HttpURLConnection con = (HttpURLConnection) url.openConnection();
                        con.setRequestMethod("GET");
                        con.connect();
                        in = con.getInputStream();
                        Row r = fill();
                        if (r != null) {
                            nextRow = r;
                            break;
                        }
                    } catch (FileNotFoundException ignored) {
                    }

                    currentRangeIndex++;
                }
            } catch (IOException ioe) {
                if (in != null) {
                    try {
                        in.close();
                    } catch (Exception ignored) {
                    }
                    in = null;
                }
                atEnd = true;
            }
        }

        synchronized Row fill() {
            try {
                return Row.readFrom(in);
            } catch (Exception e) {
                return null;
            }
        }

        public synchronized void close() {
            if (in != null) {
                try {
                    in.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                in = null;
            }
        }

        public synchronized Row next() {
            if (atEnd) {
                return null;
            }
            Row r = nextRow;
            nextRow = fill();
            while ((nextRow == null) && !atEnd) {
                currentRangeIndex++;
                openConnectionAndFill();
            }

            return r;
        }

        public synchronized boolean hasNext() {
            return !atEnd;
        }
    }

    synchronized void downloadWorkers() throws IOException {
        String result = new String(HTTP.doRequest("GET", "http://" + master + "/workers", null).body());
        String[] pieces = result.split("\n");
        int numWorkers = Integer.parseInt(pieces[0]);
        if (numWorkers < 1) {
            throw new IOException("No active KVS workers");
        }
        if (pieces.length != (numWorkers + 1)) {
            throw new RuntimeException("Received truncated response when asking KVS master for list of workers");
        }
        workers.clear();
        for (int i = 0; i < numWorkers; i++) {
            String[] pcs = pieces[1 + i].split(",");
            workers.add(new WorkerEntry(pcs[1], pcs[0]));
        }
        Collections.sort(workers);

        haveWorkers = true;
    }

    int workerIndexForKey(String key) {
        int chosenWorker = workers.size() - 1;
        if (key != null) {
            for (int i = 0; i < workers.size() - 1; i++) {
                if ((key.compareTo(workers.elementAt(i).id) >= 0) && (key.compareTo(workers.elementAt(i + 1).id) < 0)) {
                    chosenWorker = i;
                }
            }
        }

        return chosenWorker;
    }

    public KVSClient(String masterArg) {
        master = masterArg;
        workers = new Vector<>();
        haveWorkers = false;
    }

    public void rename(String oldTableName, String newTableName) throws IOException {
        if (!haveWorkers) {
            downloadWorkers();
        }

        for (WorkerEntry w : workers) {
            try {
                HTTP.doRequest("PUT", "http://" + w.address + "/rename/" + URLEncoder.encode(oldTableName, StandardCharsets.UTF_8) + "/", newTableName.getBytes()).body();
            } catch (Exception ignored) {
            }
        }
    }

    public void delete(String oldTableName) throws IOException {
        if (!haveWorkers) {
            downloadWorkers();
        }

        for (WorkerEntry w : workers) {
            try {
                HTTP.doRequest("PUT", "http://" + w.address + "/delete/" + URLEncoder.encode(oldTableName, StandardCharsets.UTF_8) + "/", null).body();
            } catch (Exception ignored) {
            }
        }
    }

    public void put(String tableName, String row, String column, byte[] value) throws IOException {
        if (!haveWorkers) {
            downloadWorkers();
        }

        try {
            String target = "http://" + workers.elementAt(workerIndexForKey(row)).address + "/data/" + URLDecoder.decode(tableName, StandardCharsets.UTF_8) + "/" + java.net.URLEncoder.encode(row, StandardCharsets.UTF_8) + "/" + java.net.URLEncoder.encode(column, StandardCharsets.UTF_8);
            byte[] response = HTTP.doRequest("PUT", target, value).body();
            String result = new String(response);
            if (!result.equals("OK")) {
                throw new RuntimeException("PUT returned something other than OK: " + result + "(" + target + ")");
            }
        } catch (UnsupportedEncodingException uee) {
            throw new RuntimeException("UTF-8 encoding not supported?!?");
        }
    }

    public void put(String tableName, String row, String column, String value) throws IOException {
        put(tableName, row, column, value.getBytes());
    }

    public void putRow(String tableName, Row row) throws IOException {
        if (!haveWorkers) {
            downloadWorkers();
        }

        byte[] response = HTTP.doRequest("PUT", "http://" + workers.elementAt(workerIndexForKey(row.key())).address + "/data/" + URLDecoder.decode(tableName, StandardCharsets.UTF_8), row.toByteArray()).body();
        String result = new String(response);
        if (!result.equals("OK")) {
            throw new RuntimeException("PUT returned something other than OK: " + result);
        }
    }

    public void putQueuedRows(String tableName, Queue<Row> rows) throws IOException {
        putQueuedRows(tableName, rows, false);
    }

    public void putQueuedRows(String tableName, Queue<Row> rows, boolean shouldAppend) throws IOException {
        if (!haveWorkers) {
            downloadWorkers();
        }

        List<List<Row>> buffer = new ArrayList<>();
        int size = rows.size();
        // rows might be modified by multiple threads, in which case it is okay that we only flush part of the queue
        for (int index = 0; index < size; index++) {
            Row row = rows.poll();
            if (row == null) {
                break;
            }
            int workerIndex = workerIndexForKey(row.key());
            while (buffer.size() <= workerIndex) {
                buffer.add(new ArrayList<>());
            }
            buffer.get(workerIndex).add(row);
        }
        for (int index = 0; index < buffer.size(); index++) {
            List<Row> rowsToPut = buffer.get(index);
            buffer.set(index, null); // help GC
            if (rowsToPut.size() > 0) {
                final byte[] LF = "\n".getBytes();
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                for (Row row : rowsToPut) {
                    baos.write(row.toByteArray());
                    baos.write(LF);
                }
                byte[] response = HTTP.doRequest("PUT",
                        "http://" + workers.elementAt(index).address + "/data/" + URLDecoder.decode(tableName, StandardCharsets.UTF_8) + (shouldAppend ? "?extra=append" : ""),
                        baos.toByteArray()
                ).body();
                String result = new String(response);
                if (!result.equals("OK")) {
                    System.err.println("PUT returned something other than OK: " + result);
                }
            }
        }
    }

    public Row getRow(String tableName, String row) throws IOException {
        if (!haveWorkers) {
            downloadWorkers();
        }

        HTTP.Response resp = HTTP.doRequest("GET", "http://" + workers.elementAt(workerIndexForKey(row)).address + "/data/" + URLDecoder.decode(tableName, StandardCharsets.UTF_8) + "/" + java.net.URLEncoder.encode(row, StandardCharsets.UTF_8), null);
        if (resp.statusCode() == 404) {
            return null;
        }

        byte[] result = resp.body();
        try {
            return Row.readFrom(new ByteArrayInputStream(result));
        } catch (Exception e) {
            throw new RuntimeException("Decoding error while reading Row from getRow() URL");
        }
    }

    public List<Row> getRows(String tableName, List<String> rowKeys) throws Exception {
        if (!haveWorkers) {
            downloadWorkers();
        }

        List<Row> result = new ArrayList<>();
        List<List<String>> buffer = new ArrayList<>();
        for (String rowKey : rowKeys) {
            if (rowKey == null) {
                continue;
            }
            int workerIndex = workerIndexForKey(rowKey);
            while (buffer.size() <= workerIndex) {
                buffer.add(new ArrayList<>());
            }
            buffer.get(workerIndex).add(rowKey);
        }
        for (int index = 0; index < buffer.size(); index++) {
            List<String> rowKeysToGet = buffer.get(index);
            if (rowKeysToGet.size() > 0) {
                HTTP.Response response = HTTP.doRequest("GET", "http://" + workers.elementAt(index).address + "/data/multirow/" + URLDecoder.decode(tableName, StandardCharsets.UTF_8) + "/" +
                                URLEncoder.encode(String.join("@!#", rowKeysToGet), StandardCharsets.UTF_8),
                        null
                );
                if (response.statusCode() == 404) {
                    continue;
                }
                byte[] responseBytes = response.body();
                ByteArrayInputStream bais = new ByteArrayInputStream(responseBytes);
                for (int j = 0; j < rowKeysToGet.size(); j++) {
                    Row row = Row.readFrom(bais);
                    if (row == null) {
                        continue;
                    }
                    result.add(row);
                }
            }
        }
        return result;
    }

    public byte[] get(String tableName, String row, String column) throws IOException {
        if (!haveWorkers) {
            downloadWorkers();
        }

        HTTP.Response res = HTTP.doRequest("GET", "http://" + workers.elementAt(workerIndexForKey(row)).address + "/data/" + URLDecoder.decode(tableName, StandardCharsets.UTF_8) + "/" + java.net.URLEncoder.encode(row, StandardCharsets.UTF_8) + "/" + java.net.URLEncoder.encode(column, StandardCharsets.UTF_8), null);
        return ((res != null) && (res.statusCode() == 200)) ? res.body() : null;
    }

    public boolean existsRow(String tableName, String row) throws IOException {
        if (!haveWorkers) {
            downloadWorkers();
        }

        HTTP.Response r = HTTP.doRequest("GET", "http://" + workers.elementAt(workerIndexForKey(row)).address + "/data/" + URLDecoder.decode(tableName, StandardCharsets.UTF_8) + "/" + java.net.URLEncoder.encode(row, StandardCharsets.UTF_8), null);
        return r.statusCode() == 200;
    }

    public int count(String tableName) throws IOException {
        if (!haveWorkers) {
            downloadWorkers();
        }

        int total = 0;
        for (WorkerEntry w : workers) {
            HTTP.Response r = HTTP.doRequest("GET", "http://" + w.address + "/count/" + URLDecoder.decode(tableName, StandardCharsets.UTF_8), null);
            if ((r != null) && (r.statusCode() == 200)) {
                String result = new String(r.body());
                total += Integer.parseInt(result);
            }
        }
        return total;
    }

    public void persist(String tableName) throws IOException {
        if (!haveWorkers) {
            downloadWorkers();
        }

        for (WorkerEntry w : workers) {
            HTTP.doRequest("PUT", "http://" + w.address + "/persist/" + URLDecoder.decode(tableName, StandardCharsets.UTF_8), null);
        }
    }

    public KVSIterator scan(String tableName) throws IOException {
        return scan(tableName, null, null);
    }

    public KVSIterator scan(String tableName, String startRow, String endRowExclusive) throws IOException {
        if (!haveWorkers) {
            downloadWorkers();
        }

        return new KVSIterator(tableName, startRow, endRowExclusive);
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Syntax: client <master> get <tableName> <row> <column>");
            System.err.println("Syntax: client <master> put <tableName> <row> <column> <value>");
            System.err.println("Syntax: client <master> scan <tableName>");
            System.err.println("Syntax: client <master> rename <oldTableName> <newTableName>");
            System.err.println("Syntax: client <master> persist <tableName>");
            System.exit(1);
        }

        KVSClient client = new KVSClient(args[0]);
        switch (args[1]) {
            case "put" -> {
                if (args.length != 6) {
                    System.err.println("Syntax: client <master> put <tableName> <row> <column> <value>");
                    System.exit(1);
                }
                client.put(args[2], args[3], args[4], args[5].getBytes(StandardCharsets.UTF_8));
            }
            case "get" -> {
                if (args.length != 5) {
                    System.err.println("Syntax: client <master> get <tableName> <row> <column>");
                    System.exit(1);
                }
                byte[] val = client.get(args[2], args[3], args[4]);
                if (val == null) {
                    System.err.println("No value found");
                } else {
                    System.out.write(val);
                }
            }
            case "scan" -> {
                if (args.length != 3) {
                    System.err.println("Syntax: client <master> scan <tableName>");
                    System.exit(1);
                }
                KVSIterator iter = client.scan(args[2], null, null);
                while (iter.hasNext()) {
                    System.out.println(iter.next());
                }
                iter.close();
            }
            case "rename" -> {
                if (args.length != 4) {
                    System.err.println("Syntax: client <master> rename <oldTableName> <newTableName>");
                    System.exit(1);
                }
                client.rename(args[2], args[3]);
            }
            case "persist" -> {
                if (args.length != 3) {
                    System.err.println("Syntax: persist <master> persist <tableName>");
                    System.exit(1);
                }
                client.persist(args[2]);
            }
            default -> {
                System.err.println("Unknown command: " + args[1]);
                System.exit(1);
            }
        }
    }
}