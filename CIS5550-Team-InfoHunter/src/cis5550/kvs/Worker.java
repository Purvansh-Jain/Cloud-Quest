package cis5550.kvs;

import cis5550.generic.InMemoryTableInfo;
import cis5550.generic.PersistentTableInfo;
import cis5550.generic.TableInfo;
import cis5550.tools.HTTP;
import cis5550.tools.URLParser;
import cis5550.webserver.Server;

import java.io.*;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class Worker extends cis5550.generic.Worker {
    private static final String idCharset = "abcdefghijklmnopqrstuvwxyz";

    private static final Map<String, TableInfo> tables = new ConcurrentHashMap<>();

    private static final AtomicBoolean isPersistentTableModified = new AtomicBoolean(false);

    public static final Boolean enableCompaction = false;

    public static final Boolean enableReplication = false;

    public static String storageDir;

    public static void main(String[] args) {
        port = Integer.parseInt(args[0]);
        storageDir = args[1];
        masterAddress = args[2];
        try {
            Path idFilePath = Path.of(storageDir, "id");
            if (Files.isRegularFile(idFilePath)) {
                id = Files.readString(idFilePath);
            } else {
                StringBuilder idBuilder = new StringBuilder();
                Random random = new Random();
                for (int i = 0; i < 5; i++) {
                    idBuilder.append(idCharset.charAt(random.nextInt(idCharset.length())));
                }
                id = idBuilder.toString();
                if (!Files.exists(idFilePath)) {
                    Files.createDirectories(idFilePath.getParent());
                    Files.createFile(idFilePath);
                }
                Files.writeString(idFilePath, id);
            }

            File folder = new File(storageDir);
            File[] files = folder.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isFile()) {
                        String fileName = file.getName();
                        if (fileName.endsWith(".table")) {
                            String tableName = fileName.substring(0, fileName.length() - 6);
                            PersistentTableInfo tableInfo = new PersistentTableInfo();
                            tableInfo.setIsDirty(true);
                            Map<String, Long> index = tableInfo.getIndex();
                            try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r")) {
                                long filePointer, fileLength = randomAccessFile.length();
                                long count = 0;
                                for (filePointer = randomAccessFile.getFilePointer();
                                     filePointer < fileLength;
                                     filePointer = randomAccessFile.getFilePointer()) {
                                    Row row = Row.readFrom(randomAccessFile);
                                    if (row != null) {
                                        index.put(row.key(), filePointer);
                                        if ((++count) % 1000 == 0) {
                                            System.out.println("[KVS Worker] Table " + tableName + " loaded: " + ((float) filePointer / fileLength * 100) + "%");
                                        }
                                    } else {
                                        break;
                                    }
                                }
                                if (filePointer < randomAccessFile.length()) {
                                    System.out.println("[KVS Worker] Table " + tableName + " is not fully loaded");
                                } else {
                                    System.out.println("[KVS Worker] Table " + tableName + " is loaded");
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                            tables.put(tableName, tableInfo);
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        Server.port(port);
        startPingThread();
        if (enableCompaction) {
            new Thread(() -> {
                while (true) {
                    try {
                        Thread.sleep(10000);
                        if (isPersistentTableModified.get()) {
                            isPersistentTableModified.set(false);
                            continue;
                        }
                        for (Map.Entry<String, TableInfo> entry : tables.entrySet()) {
                            TableInfo tableInfo = entry.getValue();
                            if (!tableInfo.isCompactionNeeded()) {
                                continue;
                            }
                            String tableName = entry.getKey();
                            PersistentTableInfo persistentTableInfo = (PersistentTableInfo) tableInfo;
                            Map<String, Long> index = persistentTableInfo.getIndex(), newIndex = new ConcurrentHashMap<>();
                            synchronized (tableInfo) {
                                System.out.println("[KVS Worker] Compacting table " + tableName);
                                try (RandomAccessFile randomAccessFile =
                                             new RandomAccessFile(Path.of(storageDir, tableName + ".table").toString(), "r");
                                     RandomAccessFile randomAccessFileTmp =
                                             new RandomAccessFile(Path.of(storageDir, tableName + ".table.tmp").toString(), "rw");) {
                                    Set<Map.Entry<String, Long>> entries = index.entrySet();
                                    for (Map.Entry<String, Long> indexEntry : entries) {
                                        randomAccessFile.seek(indexEntry.getValue());
                                        Row row = Row.readFrom(randomAccessFile);
                                        if (row != null) {
                                            newIndex.put(row.key(), randomAccessFileTmp.getFilePointer());
                                            randomAccessFileTmp.write(row.toByteArray());
                                            randomAccessFileTmp.write('\n');
                                        }
                                    }
                                }
                                System.out.println("[KVS Worker] Compaction temp file generated for table " + tableName);
                                Files.move(Path.of(storageDir, tableName + ".table.tmp"),
                                        Path.of(storageDir, tableName + ".table"),
                                        StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                                persistentTableInfo.setIndex(newIndex);
                                persistentTableInfo.setIsDirty(false);
                                System.out.println("[KVS Worker] Compaction finished for table " + tableName);
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }).start();
        }

        Server.get("/", (req, res) -> {
            StringBuilder page = new StringBuilder();
            page.append("<!DOCTYPE html>" +
                    "<html>" +
                    "<head>" +
                    "<title>KVS Worker</title>" +
                    "</head>" +
                    "<body>" +
                    "<h1>Tables</h1>" +
                    "<table style=\"border: 1px solid;\">" +
                    "<tr style=\"border: 1px solid;\">" +
                    "<th style=\"border: 1px solid;\">Name</th>" +
                    "<th style=\"border: 1px solid;\">Size</th>" +
                    "<th style=\"border: 1px solid;\">Property</th>" +
                    "</tr>");
            for (Map.Entry<String, TableInfo> entry : tables.entrySet()) {
                String tableName = entry.getKey();
                TableInfo tableInfo = entry.getValue();
                page.append("<tr style=\"border: 1px solid;\">");
                page.append("<td style=\"border: 1px solid;\"><a href=\"/view/").append(tableName).append("\">").append(tableName).append("</a></td>");
                page.append("<td style=\"border: 1px solid;\">").append(tableInfo.getRowCount()).append("</td>");
                page.append("<td style=\"border: 1px solid;\">").append(tableInfo.isPersistent() ? "Persistent" : "-").append("</td>");
                page.append("</tr>");
            }
            page.append("</table>" +
                    "</body>" +
                    "</html>");
            return page.toString();
        });

        Server.get("/view/:table", (req, res) -> {
            res.type("text/html");
            String table = URLDecoder.decode(req.params("table"), StandardCharsets.UTF_8);
            String fromRow = URLParser.decodeNullableURLParam(req.queryParams("fromRow"));
            TableInfo tableInfo = tables.get(table);
            if (tableInfo == null) {
                res.status(404, "Not Found");
                res.body("Table not found");
                return null;
            }
            PriorityQueue<String> outputRowKeyQueue = new PriorityQueue<>(Comparator.reverseOrder());
            for (String rowKey : tableInfo.getRowKeys()) {
                if (fromRow == null || rowKey.compareTo(fromRow) >= 0) {
                    outputRowKeyQueue.add(rowKey);
                }
                if (outputRowKeyQueue.size() > 11) {
                    outputRowKeyQueue.poll();
                }
            }
            String nextRow = outputRowKeyQueue.size() > 10 ? outputRowKeyQueue.poll() : null;
            Set<String> outputColumnKeySet = new HashSet<>();
            List<Map.Entry<String, Row>> outputRows = new ArrayList<>();
            for (String rowKey : outputRowKeyQueue) {
                Row row = getRow(table, rowKey);
                outputRows.add(Map.entry(rowKey, row));
                outputColumnKeySet.addAll(row.columns());
            }
            List<String> outputColumnKeyList = new ArrayList<>(outputColumnKeySet);
            outputRows.sort(Map.Entry.comparingByKey());
            Collections.sort(outputColumnKeyList);
            StringBuilder page = new StringBuilder();
            page.append("<!DOCTYPE html>" +
                    "<html>" +
                    "<head>" +
                    "<title>KVS Worker</title>" +
                    "</head>" +
                    "<body>" +
                    "<h1>Table: ").append(table).append("</h1>" +
                    "<table style=\"border: 1px solid;\">" +
                    "<tr style=\"border: 1px solid;\">" +
                    "<th style=\"border: 1px solid;\">Row_Key</th>");
            for (String colName : outputColumnKeyList) {
                page.append("<th style=\"border: 1px solid;\">").append(colName).append("</th>");
            }
            page.append("</tr>");
            for (Map.Entry<String, Row> entry : outputRows) {
                Row row = entry.getValue();
                page.append("<tr style=\"border: 1px solid;\">").append("<td style=\"border: 1px solid;\">").append(entry.getKey()).append("</td>");
                for (String colName : outputColumnKeyList) {
                    page.append("<td style=\"border: 1px solid;\">");
                    String content = row.get(colName);
                    if (content != null && content.length() > 500) {
                        page.append("<a href=\"/data/").append(table).append("/").append(entry.getKey()).append("/").append(colName).append("\">").append("Download</a>");
                    } else {
                        page.append(content);
                    }
                    page.append("</td>");
                }
                page.append("</tr>");
            }
            page.append("</table>");
            if (nextRow != null) {
                page.append("<a href=\"/view/").append(table).append("?fromRow=")
                        .append(URLEncoder.encode(nextRow, StandardCharsets.UTF_8)).append("\">Next</a>");
            }
            page.append("</body>" +
                    "</html>");
            return page.toString();
        });

        Server.put("/persist/:table", (req, res) -> {
            String table = URLDecoder.decode(req.params("table"), StandardCharsets.UTF_8);
            if (req.queryParams("replica") == null) {
                redirectToReplicas("/persist/", Collections.singletonList(table), null);
            }
            if (tables.containsKey(table)) {
                res.status(403, "Forbidden");
                res.body("Table already exists");
                return null;
            }
            TableInfo tableInfo = tables.computeIfAbsent(table, k -> new PersistentTableInfo());
            synchronized (tableInfo) {
                if (tableInfo.isPersistent() &&
                        Path.of(storageDir, table + ".table").toFile().createNewFile()) {
                    System.out.println("Table " + table + " persisted");
                    return "OK";
                } else {
                    res.status(403, "Forbidden");
                    res.body("Table already exists");
                    return null;
                }
            }
        });

        Server.get("/data/multirow/:table/:rows", (req, res) -> {
            String table = URLDecoder.decode(req.params("table"), StandardCharsets.UTF_8);
            String rowKeys = URLDecoder.decode(req.params("rows"), StandardCharsets.UTF_8);
            res.type("application/octet-stream");
            final byte[] LF = "\n".getBytes();
            boolean hasFound = false;
            List<Row> rows = getRows(table, rowKeys.split("@!#"));
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            if (rows != null) {
                for (Row row : rows) {
                    if (row == null) {
                        continue;
                    }
                    hasFound = true;
                    byteArrayOutputStream.write(row.toByteArray());
                    byteArrayOutputStream.write(LF);
                }
            }
            if (hasFound) {
                byteArrayOutputStream.write(LF);
            } else {
                res.status(404, "Not Found");
                return null;
            }
            return byteArrayOutputStream.toByteArray();
        });

        Server.put("/data/:table/:row/:column", (req, res) -> {
            String table = URLDecoder.decode(req.params("table"), StandardCharsets.UTF_8);
            String row = URLDecoder.decode(req.params("row"), StandardCharsets.UTF_8);
            String column = URLDecoder.decode(req.params("column"), StandardCharsets.UTF_8);
            byte[] value = req.bodyAsBytes();
            if (req.queryParams("replica") == null) {
                redirectToReplicas("/data/", List.of(table, row, column), value);
            }
            if (putValue(table, row, column, value)) {
                return "OK";
            } else {
                res.status(400, "Bad Request");
                res.body("FAIL");
                return null;
            }
        });

        Server.put("/data/:table", (req, res) -> {
            String table = URLDecoder.decode(req.params("table"), StandardCharsets.UTF_8);
            String extra = URLParser.decodeNullableURLParam(req.queryParams("extra"));
            boolean shouldAppend = extra != null && extra.contains("append");
            byte[] value = req.bodyAsBytes();
            if (req.queryParams("replica") == null) {
                redirectToReplicas("/data/", Collections.singletonList(table), value);
            }
            ByteArrayInputStream inputStream = new ByteArrayInputStream(value);
            List<Row> rows = new ArrayList<>();
            while (inputStream.available() > 0) {
                Row row = Row.readFrom(inputStream);
                if (row != null) {
                    rows.add(row);
                } else {
                    break;
                }
            }
            if (putRows(table, rows, shouldAppend)) {
                return "OK";
            } else {
                res.status(400, "Bad Request");
                res.body("FAIL");
                return null;
            }
        });
        // this will replace the existing table if target table exists
        Server.put("/rename/:table", (req, res) -> {
            String table = URLDecoder.decode(req.params("table"), StandardCharsets.UTF_8);
            String newName = req.body();
            if (req.queryParams("replica") == null) {
                redirectToReplicas("/rename/", Collections.singletonList(table), req.bodyAsBytes());
            }
            TableInfo tableInfo = tables.get(table);
            if (tableInfo == null) {
                res.status(404, "Not Found");
                return null;
            }
            if (tables.computeIfAbsent(newName, k -> tableInfo) != tableInfo) {
                System.out.println("Conflict Table already exists");
                res.status(409, "Conflict");
                res.body("Table already exists");
                return null;
            }
            if (tableInfo.isPersistent()) {
                synchronized (tableInfo) {
                    Path oldPath = Path.of(storageDir, table + ".table");
                    Path newPath = Path.of(storageDir, newName + ".table");
                    try {
                        System.out.println("Renaming " + oldPath + " to " + newPath);
                        Files.move(oldPath, newPath, StandardCopyOption.ATOMIC_MOVE);
                        tables.remove(table);
                        return "OK";
                    } catch (FileNotFoundException e) {
                        System.out.println(table + "File not found");
                        res.status(404, "Not Found");
                        res.body("Table not found");
                        return null;
                    } catch (FileAlreadyExistsException e) {
                        System.out.println("File already exists");
                        res.status(409, "Conflict");
                        res.body("Table already exists");
                        return null;
                    }
                }
            } else { // not persistent
                System.out.println("Table " + table + " renamed to " + newName);
                tables.remove(table);
                return "OK";
            }
        });

        Server.put("/delete/:table", (req, res) -> {
            String table = URLDecoder.decode(req.params("table"), StandardCharsets.UTF_8);
            if (req.queryParams("replica") == null) {
                redirectToReplicas("/delete/", Collections.singletonList(table), null);
            }
            TableInfo tableInfo = tables.remove(table);
            if (tableInfo == null) {
                res.status(404, "Not Found");
                return null;
            }
            if (tableInfo.isPersistent()) {
                synchronized (tableInfo) {
                    Path path = Path.of(storageDir, table + ".table");
                    if (!path.toFile().delete()) {
                        res.status(400, "Bad Request");
                        res.body("FAIL");
                        return null;
                    }
                }
            }
            System.out.println("Table " + table + " deleted");
            return "OK";
        });

        Server.get("/tables", (req, res) -> {
            res.type("text/plain");
            res.body(String.join("\n", tables.keySet()) + "\n");
            return null;
        });

        Server.get("/count/:table", (req, res) -> {
            String table = URLDecoder.decode(req.params("table"), StandardCharsets.UTF_8);
            TableInfo tableInfo = tables.get(table);
            if (tableInfo == null) {
                res.status(404, "Not Found");
                return null;
            }
            res.type("text/plain");
            res.body(String.valueOf(tableInfo.getRowKeys().size()));
            return null;
        });

        Server.get("/data/:table", (req, res) -> {
            String table = URLDecoder.decode(req.params("table"), StandardCharsets.UTF_8);
            String startRow = req.queryParams("startRow");
            String endRowExclusive = req.queryParams("endRowExclusive");
            TableInfo tableInfo = tables.get(table);
            if (tableInfo == null) {
                res.status(404, "Not Found");
                return null;
            }
            res.type("application/octet-stream");
            final byte[] LF = "\n".getBytes();
            boolean hasFound = false;
            for (String rowKey : tableInfo.getRowKeys()) {
                boolean outOfRange = false;
                if (startRow != null && endRowExclusive == null) {
                    outOfRange = rowKey.compareTo(startRow) < 0;
                } else if (startRow == null && endRowExclusive != null) {
                    outOfRange = rowKey.compareTo(endRowExclusive) >= 0;
                } else if (startRow != null) {
                    outOfRange = startRow.compareTo(endRowExclusive) < 0 ?
                            rowKey.compareTo(startRow) < 0 || rowKey.compareTo(endRowExclusive) >= 0 :
                            rowKey.compareTo(startRow) < 0 && rowKey.compareTo(endRowExclusive) >= 0;
                }
                if (outOfRange) {
                    System.out.println("[KVS Worker] Skipped row " + rowKey + " because it is out of range, startRow=" + startRow + ", endRowExclusive=" + endRowExclusive);
                    continue;
                }
                hasFound = true;
                res.write(getRow(table, rowKey).toByteArray());
                res.write(LF);
            }
            if (hasFound) {
                res.write(LF);
            } else {
                res.status(404, "Not Found");
            }
            return null;
        });

        Server.get("/data/:table/:row", (req, res) -> {
            String table = URLDecoder.decode(req.params("table"), StandardCharsets.UTF_8);
            String row = URLDecoder.decode(req.params("row"), StandardCharsets.UTF_8);
            Row destRow = getRow(table, row);
            res.type("application/octet-stream");
            if (destRow == null) {
                res.status(404, "Not Found");
            } else {
                res.bodyAsBytes(destRow.toByteArray());
            }
            return null;
        });

        Server.get("/data/:table/:row/:column", (req, res) -> {
            String table = URLDecoder.decode(req.params("table"), StandardCharsets.UTF_8);
            String row = URLDecoder.decode(req.params("row"), StandardCharsets.UTF_8);
            String column = URLDecoder.decode(req.params("column"), StandardCharsets.UTF_8);
            Row destRow = getRow(table, row);
            if (destRow == null) {
                res.status(404, "Not Found");
                return null;
            }
            byte[] bytes = destRow.getBytes(column);
            if (bytes == null) {
                res.status(404, "Not Found");
                return null;
            }
            res.type("application/octet-stream");
            res.bodyAsBytes(bytes);
            return null;
        });
    }

    static Row getRow(String table, String key) {
        if (table == null || key == null) {
            return null;
        }
        TableInfo tableInfo = tables.get(table);
        if (tableInfo == null) {
            return null;
        }
        if (!tableInfo.isPersistent()) {
            return ((InMemoryTableInfo) tableInfo).getData().get(key);
        } else {
            synchronized (tableInfo) {
                Long offset = ((PersistentTableInfo) tableInfo).getIndex().get(key);
                if (offset == null) {
                    return null;
                }
                try (RandomAccessFile file = new RandomAccessFile(
                        Path.of(storageDir, table + ".table").toFile(), "r")) {
                    file.seek(offset);
                    return Row.readFrom(file);
                } catch (Exception e) {
                    e.printStackTrace();
                    return null;
                }
            }
        }
    }

    static List<Row> getRows(String table, String[] keys) {
        if (table == null || keys == null) {
            return null;
        }
        TableInfo tableInfo = tables.get(table);
        if (tableInfo == null) {
            return null;
        }
        if (!tableInfo.isPersistent()) {
            Map<String, Row> data = ((InMemoryTableInfo) tableInfo).getData();
            return Arrays.stream(keys).map(data::get).toList();
        } else {
            List<Row> result = new ArrayList<>();
            synchronized (tableInfo) {
                try (RandomAccessFile file = new RandomAccessFile(
                        Path.of(storageDir, table + ".table").toFile(), "r")) {
                    for (String key : keys) {
                        Long offset = ((PersistentTableInfo) tableInfo).getIndex().get(key);
                        if (offset == null) {
                            result.add(null);
                            continue;
                        }
                        file.seek(offset);
                        result.add(Row.readFrom(file));
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    return null;
                }
            }
            return result;
        }
    }

    static boolean putValue(String table, String rowKey, String columnKey, byte[] value) {
        TableInfo tableInfo = tables.computeIfAbsent(table, k -> new InMemoryTableInfo());
        if (tableInfo.isPersistent()) {
            isPersistentTableModified.set(true);
            synchronized (tableInfo) {
                PersistentTableInfo persistentTableInfo = (PersistentTableInfo) tableInfo;
                persistentTableInfo.setIsDirty(true);
                Long oldOffset = persistentTableInfo.getIndex().get(rowKey);
                try (RandomAccessFile file = new RandomAccessFile(
                        Path.of(storageDir, table + ".table").toFile(), "rw")) {
                    Row draftRow;
                    if (oldOffset == null) {
                        draftRow = new Row(rowKey);
                    } else {
                        file.seek(oldOffset);
                        draftRow = Row.readFrom(file);
                    }
                    draftRow.put(columnKey, value);
                    long newOffset = file.length();
                    file.seek(newOffset);
                    file.write(draftRow.toByteArray());
                    file.write('\n');
                    persistentTableInfo.getIndex().put(rowKey, newOffset);
                    return true;
                } catch (Exception e) {
                    e.printStackTrace();
                    return false;
                }
            }
        } else {
            ((InMemoryTableInfo) tableInfo).getData()
                    .computeIfAbsent(rowKey, k -> new Row(rowKey)).put(columnKey, value);
            return true;
        }
    }

    static boolean putRows(String table, List<Row> rows) {
        return putRows(table, rows, false);
    }

    static boolean putRows(String table, List<Row> rows, boolean shouldAppend) {
        TableInfo tableInfo = tables.computeIfAbsent(table, k -> new InMemoryTableInfo());
        if (tableInfo.isPersistent()) {
            isPersistentTableModified.set(true);
            synchronized (tableInfo) {
                PersistentTableInfo persistentTableInfo = (PersistentTableInfo) tableInfo;
                persistentTableInfo.setIsDirty(true);
                Map<String, Long> index = persistentTableInfo.getIndex();
                try (RandomAccessFile file = new RandomAccessFile(
                        Path.of(storageDir, table + ".table").toFile(), "rw"
                )) {
                    file.seek(file.length());
                    if (shouldAppend) {
                        System.out.println("Appending to " + table + ".table");
                    }
                    for (Row row : rows) {
                        Row rowToWrite = row;   // local variable helps GC
                        long currentOffset = file.getFilePointer();
                        if (shouldAppend) {
                            Long oldOffset = index.get(row.key());
                            if (oldOffset != null) {
                                file.seek(oldOffset);
                                rowToWrite = Row.readFrom(file);
                                file.seek(currentOffset);
                                if (rowToWrite == null) {
                                    rowToWrite = row;
                                } else {
                                    for (String column : row.columns()) {
                                        rowToWrite.compute(column, (k, v) -> {
                                            if (v == null) {
                                                return row.get(column);
                                            } else {
                                                return v + "@@!!##" + row.get(column);
                                            }
                                        });
                                    }
                                }
                            }
                        }
                        file.write(rowToWrite.toByteArray());
                        file.write('\n');
                        index.put(rowToWrite.key(), currentOffset);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    return false;
                }
            }
        } else {
            InMemoryTableInfo inMemoryTableInfo = (InMemoryTableInfo) tableInfo;
            for (Row row : rows) {
                inMemoryTableInfo.getData().put(row.key(), row);
            }
        }
        return true;
    }

    static void redirectToReplicas(String basePath, List<String> pathVariables, byte[] body) {
        if (!enableReplication) {
            return;
        }
        if (!basePath.endsWith("/")) {
            basePath += "/";
        }
        for (String node : replicaNodes) {
            try {
                HTTP.doRequestWithTimeout(
                        "PUT",
                        "http://" + node + basePath +
                                pathVariables
                                        .stream()
                                        .map(v -> URLEncoder.encode(v, StandardCharsets.UTF_8))
                                        .collect(Collectors.joining("/"))
                                + "?replica=true",
                        body,
                        1000
                );
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
