package cis5550.flame;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;
import cis5550.tools.RandomizedRDDIterator;
import cis5550.tools.Serializer;
import cis5550.tools.URLParser;

import java.io.File;
import java.io.FileOutputStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.StreamSupport;

import static cis5550.tools.StringUtil.generateRandomAlphabeticString;
import static cis5550.webserver.Server.port;
import static cis5550.webserver.Server.post;

class Worker extends cis5550.generic.Worker {

    final static ExecutorService threadPool = Executors.newFixedThreadPool(64);

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Syntax: Worker <port> <masterIP:port>");
            System.exit(1);
        }

        id = args[0];
        port = Integer.parseInt(id);
        masterAddress = args[1];
        startPingThread();
        final File myJAR = new File("__worker" + port + "-current.jar");

        port(port);

        post("/useJAR", (request, response) -> {
            FileOutputStream fos = new FileOutputStream(myJAR);
            fos.write(request.bodyAsBytes());
            fos.close();
            return "OK";
        });

        post("/rdd/flatMap", (request, response) -> {
            String inputTableName = URLParser.decodeNullableURLParam(request.queryParams("inputTableName"));
            String outputTableName = URLParser.decodeNullableURLParam(request.queryParams("outputTableName"));
            String kvsMaster = URLParser.decodeNullableURLParam(request.queryParams("kvsMaster"));
            String fromKey = URLParser.decodeNullableURLParam(request.queryParams("fromKey"));
            String toKey = URLParser.decodeNullableURLParam(request.queryParams("toKey"));
            String extra = URLParser.decodeNullableURLParam(request.queryParams("extra"));
            var lambda = (FlameRDD.StringToIterable) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
            KVSClient kvs = new KVSClient(kvsMaster);
            KVSClient.KVSIterator rowIterator = kvs.scan(inputTableName, fromKey, toKey);
            while (rowIterator.hasNext()) {
                Row row = rowIterator.next();
                Iterable<String> result = lambda.op(row.get("value"));
                if (result != null) {
                    for (String s : result) {
                        if (extra != null && extra.equals("hash")) {  // only used for persist flatMap and only for crawlQueue
                            String url = s.split("@!#")[0];
                            kvs.put(outputTableName, Hasher.hash(url), "value", s);
                        } else {
                            kvs.put(outputTableName, generateRandomAlphabeticString(), "value", s);
                        }
                    }
                }
            }
            rowIterator.close();
            return "OK";
        });


        post("/rdd/flatMap-random", (request, response) -> {
            String inputTableName = URLParser.decodeNullableURLParam(request.queryParams("inputTableName"));
            String outputTableName = URLParser.decodeNullableURLParam(request.queryParams("outputTableName"));
            String kvsMaster = URLParser.decodeNullableURLParam(request.queryParams("kvsMaster"));
            String fromKey = URLParser.decodeNullableURLParam(request.queryParams("fromKey"));
            String toKey = URLParser.decodeNullableURLParam(request.queryParams("toKey"));
            String extra = URLParser.decodeNullableURLParam(request.queryParams("extra"));
            var lambda = (FlameRDD.StringToIterable) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
            KVSClient kvs = new KVSClient(kvsMaster);
            KVSClient.KVSIterator rowIterator = kvs.scan(inputTableName, fromKey, toKey);
            Iterator<String> rddIterator = new RandomizedRDDIterator(rowIterator);

            ConcurrentLinkedDeque<Row> rowBuffer = new ConcurrentLinkedDeque<>();
            Map<Integer, String> unfinishedOps = new ConcurrentHashMap<>();
            int id = 0;
            while (rddIterator.hasNext()) {
                int finalId = ++id;
                String value = rddIterator.next();
                while (unfinishedOps.size() > 4096) {
                    Thread.sleep(500);
                }
                unfinishedOps.put(finalId, value);
                threadPool.submit(() -> {
                    try {
                        Iterable<String> result = lambda.op(value);
                        if (result != null) {
                            rowBuffer.addAll(
                                    StreamSupport.stream(result.spliterator(), false)
                                            .map(
                                                    extra != null && extra.equals("hash") ?
                                                            s -> {
                                                                Row row = new Row(Hasher.hash(s.split("@!#")[0]));
                                                                row.put("value", s);
                                                                return row;
                                                            } :
                                                            s -> {
                                                                Row row = new Row(generateRandomAlphabeticString());
                                                                row.put("value", s);
                                                                return row;
                                                            }
                                            )
                                            .toList()
                            );
                            if (rowBuffer.size() > 8192) {
                                kvs.putQueuedRows(outputTableName, rowBuffer);
                            }
                        }
                    } catch (Exception e) {
                        System.out.println("[Flame Worker] flatMap op Failed" + " id: " + finalId + ", url: " + value);
                        e.printStackTrace();
                    } finally {
                        unfinishedOps.remove(finalId);
                    }
                });
            }
            if (!unfinishedOps.isEmpty()) {
                Thread.sleep(5000);
                while (!unfinishedOps.isEmpty()) {
                    if (unfinishedOps.size() < 32) {
                        System.out.println("[Flame Worker] Waiting for unfinished tasks: " + unfinishedOps);
                    } else {
                        System.out.println("[Flame Worker] Waiting for " + unfinishedOps.size() + " unfinished tasks");
                    }
                    Thread.sleep(10000);
                }
            }
            kvs.putQueuedRows(outputTableName, rowBuffer); // flush remaining rows
            rowIterator.close();
            System.out.println("[Flame Worker] flatMap Request Completed, count: " + id);
            return "OK";
        });

        post("/rdd/flatMapToPair", (request, response) -> {
            String inputTableName = URLParser.decodeNullableURLParam(request.queryParams("inputTableName"));
            String outputTableName = URLParser.decodeNullableURLParam(request.queryParams("outputTableName"));
            String kvsMaster = URLParser.decodeNullableURLParam(request.queryParams("kvsMaster"));
            String fromKey = URLParser.decodeNullableURLParam(request.queryParams("fromKey"));
            String toKey = URLParser.decodeNullableURLParam(request.queryParams("toKey"));
            var lambda = (FlameRDD.StringToPairIterable) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
            KVSClient kvs = new KVSClient(kvsMaster);
            KVSClient.KVSIterator rowIterator = kvs.scan(inputTableName, fromKey, toKey);
            while (rowIterator.hasNext()) {
                Row row = rowIterator.next();
                Iterable<FlamePair> result = lambda.op(row.get("value"));
                if (result != null) {
                    for (FlamePair flamePair : result) {
                        kvs.put(outputTableName, flamePair.a, generateRandomAlphabeticString(), flamePair.b);
                    }
                }
            }
            rowIterator.close();
            return "OK";
        });

        post("/rdd/flatMap-pair", (request, response) -> {
            String inputTableName = URLParser.decodeNullableURLParam(request.queryParams("inputTableName"));
            String outputTableName = URLParser.decodeNullableURLParam(request.queryParams("outputTableName"));
            String kvsMaster = URLParser.decodeNullableURLParam(request.queryParams("kvsMaster"));
            String fromKey = URLParser.decodeNullableURLParam(request.queryParams("fromKey"));
            String toKey = URLParser.decodeNullableURLParam(request.queryParams("toKey"));
            var lambda = (FlamePairRDD.PairToStringIterable) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
            KVSClient kvs = new KVSClient(kvsMaster);
            KVSClient.KVSIterator rowIterator = kvs.scan(inputTableName, fromKey, toKey);
            while (rowIterator.hasNext()) {
                Row row = rowIterator.next();
                for (String column : row.columns()) {
                    Iterable<String> result = lambda.op(new FlamePair(row.key(), row.get(column)));
                    if (result != null) {
                        for (String s : result) {
                            if (s != null) {
                                kvs.put(outputTableName, generateRandomAlphabeticString(), "value", s);
                            }
                        }
                    }
                }
            }
            rowIterator.close();
            return "OK";
        });

        post("/rdd/flatMapToPair-pair", (request, response) -> {
            String inputTableName = URLParser.decodeNullableURLParam(request.queryParams("inputTableName"));
            String outputTableName = URLParser.decodeNullableURLParam(request.queryParams("outputTableName"));
            String kvsMaster = URLParser.decodeNullableURLParam(request.queryParams("kvsMaster"));
            String fromKey = URLParser.decodeNullableURLParam(request.queryParams("fromKey"));
            String toKey = URLParser.decodeNullableURLParam(request.queryParams("toKey"));
            var lambda = (FlamePairRDD.PairToPairIterable) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
            KVSClient kvs = new KVSClient(kvsMaster);
            KVSClient.KVSIterator rowIterator = kvs.scan(inputTableName, fromKey, toKey);
            while (rowIterator.hasNext()) {
                Row row = rowIterator.next();
                for (String column : row.columns()) {
                    Iterable<FlamePair> result = lambda.op(new FlamePair(row.key(), row.get(column)));
                    if (result != null) {
                        for (FlamePair flamePair : result) {
                            if (flamePair != null) {
                                kvs.put(outputTableName, flamePair.a, generateRandomAlphabeticString(), flamePair.b);
                            }
                        }
                    }
                }
            }
            rowIterator.close();
            return "OK";
        });

        post("/rdd/mapToPair", (request, response) -> {
            String inputTableName = URLParser.decodeNullableURLParam(request.queryParams("inputTableName"));
            String outputTableName = URLParser.decodeNullableURLParam(request.queryParams("outputTableName"));
            String kvsMaster = URLParser.decodeNullableURLParam(request.queryParams("kvsMaster"));
            String fromKey = URLParser.decodeNullableURLParam(request.queryParams("fromKey"));
            String toKey = URLParser.decodeNullableURLParam(request.queryParams("toKey"));
            var lambda = (FlameRDD.StringToPair) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
            KVSClient kvs = new KVSClient(kvsMaster);
            KVSClient.KVSIterator rowIterator = kvs.scan(inputTableName, fromKey, toKey);
            while (rowIterator.hasNext()) {
                Row row = rowIterator.next();
                FlamePair result = lambda.op(row.get("value"));
                if (result != null) {
                    kvs.put(outputTableName, result.a, generateRandomAlphabeticString(), result.b);
                }
            }
            rowIterator.close();
            return "OK";
        });

        post("/rdd/distinct", (request, response) -> {
            String inputTableName = URLParser.decodeNullableURLParam(request.queryParams("inputTableName"));
            String outputTableName = URLParser.decodeNullableURLParam(request.queryParams("outputTableName"));
            String kvsMaster = URLParser.decodeNullableURLParam(request.queryParams("kvsMaster"));
            String fromKey = URLParser.decodeNullableURLParam(request.queryParams("fromKey"));
            String toKey = URLParser.decodeNullableURLParam(request.queryParams("toKey"));
            KVSClient kvs = new KVSClient(kvsMaster);
            KVSClient.KVSIterator rowIterator = kvs.scan(inputTableName, fromKey, toKey);
            while (rowIterator.hasNext()) {
                Row row = rowIterator.next();
                String value = row.get("value");
                kvs.put(outputTableName, value, "value", value);
            }
            rowIterator.close();
            return "OK";
        });

        post("/rdd/fold", (request, response) -> {
            String inputTableName = URLParser.decodeNullableURLParam(request.queryParams("inputTableName"));
            String kvsMaster = URLParser.decodeNullableURLParam(request.queryParams("kvsMaster"));
            String fromKey = URLParser.decodeNullableURLParam(request.queryParams("fromKey"));
            String toKey = URLParser.decodeNullableURLParam(request.queryParams("toKey"));
            String accumulator = URLParser.decodeNullableURLParam(request.queryParams("extra"));
            var lambda = (FlamePairRDD.TwoStringsToString) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
            KVSClient kvs = new KVSClient(kvsMaster);
            KVSClient.KVSIterator rowIterator = kvs.scan(inputTableName, fromKey, toKey);
            while (rowIterator.hasNext()) {
                Row row = rowIterator.next();
                accumulator = lambda.op(accumulator, row.get("value"));
            }
            rowIterator.close();
            return accumulator;
        });

        post("/rdd/foldByKey-pair", (request, response) -> {
            String inputTableName = URLParser.decodeNullableURLParam(request.queryParams("inputTableName"));
            String outputTableName = URLParser.decodeNullableURLParam(request.queryParams("outputTableName"));
            String kvsMaster = URLParser.decodeNullableURLParam(request.queryParams("kvsMaster"));
            String fromKey = URLParser.decodeNullableURLParam(request.queryParams("fromKey"));
            String toKey = URLParser.decodeNullableURLParam(request.queryParams("toKey"));
            String zeroElement = URLParser.decodeNullableURLParam(request.queryParams("extra"));
            var lambda = (FlamePairRDD.TwoStringsToString) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
            KVSClient kvs = new KVSClient(kvsMaster);
            KVSClient.KVSIterator rowIterator = kvs.scan(inputTableName, fromKey, toKey);
            while (rowIterator.hasNext()) {
                Row row = rowIterator.next();
                String accumulator = zeroElement;
                for (String column : row.columns()) {
                    accumulator = lambda.op(accumulator, row.get(column));
                }
                if (accumulator != null) {
                    kvs.put(outputTableName, row.key(), generateRandomAlphabeticString(), accumulator);
                }
            }
            rowIterator.close();
            return "OK";
        });

        post("/rdd/intersection", (request, response) -> {
            String inputTableName = URLParser.decodeNullableURLParam(request.queryParams("inputTableName"));
            String outputTableName = URLParser.decodeNullableURLParam(request.queryParams("outputTableName"));
            String kvsMaster = URLParser.decodeNullableURLParam(request.queryParams("kvsMaster"));
            String fromKey = URLParser.decodeNullableURLParam(request.queryParams("fromKey"));
            String toKey = URLParser.decodeNullableURLParam(request.queryParams("toKey"));
            String otherTableName = URLParser.decodeNullableURLParam(request.queryParams("extra"));
            KVSClient kvs = new KVSClient(kvsMaster);
            KVSClient.KVSIterator rowIterator1 = kvs.scan(inputTableName, fromKey, toKey);
            InputTableIter:
            while (rowIterator1.hasNext()) {
                Row row1 = rowIterator1.next();
                String value1 = row1.get("value");
                String outputKey = String.valueOf(value1.hashCode());
                while (kvs.existsRow(outputTableName, outputKey)) {
                    byte[] currValue = kvs.get(outputTableName, outputKey, "value");
                    if (currValue == null) {
                        break;
                    }
                    if (Arrays.equals(value1.getBytes(), currValue)) {
                        continue InputTableIter;
                    } else {
                        outputKey += '-';
                    }
                }
                {
                    KVSClient.KVSIterator rowIterator2 = kvs.scan(otherTableName, fromKey, toKey);
                    while (rowIterator2.hasNext()) {
                        Row row2 = rowIterator2.next();
                        String value2 = row2.get("value");
                        if (value1.equals(value2)) {
                            kvs.put(outputTableName, outputKey, "value", value1);
                            continue InputTableIter;
                        }
                    }
                    rowIterator2.close();
                }
            }
            rowIterator1.close();
            return "OK";
        });

        post("/rdd/sample", (request, response) -> {
            String inputTableName = URLParser.decodeNullableURLParam(request.queryParams("inputTableName"));
            String outputTableName = URLParser.decodeNullableURLParam(request.queryParams("outputTableName"));
            String kvsMaster = URLParser.decodeNullableURLParam(request.queryParams("kvsMaster"));
            String fromKey = URLParser.decodeNullableURLParam(request.queryParams("fromKey"));
            String toKey = URLParser.decodeNullableURLParam(request.queryParams("toKey"));
            String probString = URLParser.decodeNullableURLParam(request.queryParams("extra"));
            double prob = Double.parseDouble(probString);
            KVSClient kvs = new KVSClient(kvsMaster);
            Random random = new Random();
            KVSClient.KVSIterator rowIterator = kvs.scan(inputTableName, fromKey, toKey);
            while (rowIterator.hasNext()) {
                if (random.nextDouble() < prob) {
                    Row row = rowIterator.next();
                    kvs.put(outputTableName, row.key(), "value", row.get("value"));
                } else {
                    rowIterator.next();
                }
            }
            rowIterator.close();
            return "OK";
        });

        post("/rdd/groupBy", (request, response) -> {
            String inputTableName = URLParser.decodeNullableURLParam(request.queryParams("inputTableName"));
            String outputTableName = URLParser.decodeNullableURLParam(request.queryParams("outputTableName"));
            String kvsMaster = URLParser.decodeNullableURLParam(request.queryParams("kvsMaster"));
            String fromKey = URLParser.decodeNullableURLParam(request.queryParams("fromKey"));
            String toKey = URLParser.decodeNullableURLParam(request.queryParams("toKey"));
            var lambda = (FlameRDD.StringToString) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
            KVSClient kvs = new KVSClient(kvsMaster);
            KVSClient.KVSIterator rowIterator = kvs.scan(inputTableName, fromKey, toKey);
            while (rowIterator.hasNext()) {
                Row row = rowIterator.next();
                String value = row.get("value");
                kvs.put(outputTableName, lambda.op(value), generateRandomAlphabeticString(), value);
            }
            rowIterator.close();
            return "OK";
        });

        post("/rdd/fromTable", (request, response) -> {
            String inputTableName = URLParser.decodeNullableURLParam(request.queryParams("inputTableName"));
            String outputTableName = URLParser.decodeNullableURLParam(request.queryParams("outputTableName"));
            String kvsMaster = URLParser.decodeNullableURLParam(request.queryParams("kvsMaster"));
            String fromKey = URLParser.decodeNullableURLParam(request.queryParams("fromKey"));
            String toKey = URLParser.decodeNullableURLParam(request.queryParams("toKey"));
            var lambda = (FlameContext.RowToString) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
            KVSClient kvs = new KVSClient(kvsMaster);
            KVSClient.KVSIterator rowIterator = kvs.scan(inputTableName, fromKey, toKey);
            while (rowIterator.hasNext()) {
                Row row = rowIterator.next();
                kvs.put(outputTableName, row.key(), "value", lambda.op(row));
            }
            rowIterator.close();
            return "OK";
        });

        post("/rdd/join-pair", (request, response) -> {
            String inputTableName = URLParser.decodeNullableURLParam(request.queryParams("inputTableName"));
            String outputTableName = URLParser.decodeNullableURLParam(request.queryParams("outputTableName"));
            String kvsMaster = URLParser.decodeNullableURLParam(request.queryParams("kvsMaster"));
            String fromKey = URLParser.decodeNullableURLParam(request.queryParams("fromKey"));
            String toKey = URLParser.decodeNullableURLParam(request.queryParams("toKey"));
            String otherTableName = URLParser.decodeNullableURLParam(request.queryParams("extra"));
            KVSClient kvs = new KVSClient(kvsMaster);
            KVSClient.KVSIterator rowIterator = kvs.scan(inputTableName, fromKey, toKey);
            while (rowIterator.hasNext()) {
                Row row1 = rowIterator.next();
                String key = row1.key();
                Row row2 = kvs.getRow(otherTableName, key);
                if (row2 == null) {
                    continue;
                }
                for (String column1 : row1.columns()) {
                    for (String column2 : row2.columns()) {
                        kvs.put(outputTableName, key, generateRandomAlphabeticString(),
                                row1.get(column1) + "," + row2.get(column2));
                    }
                }
            }
            rowIterator.close();
            return "OK";
        });

        post("/rdd/filter", (request, response) -> {
            String inputTableName = URLParser.decodeNullableURLParam(request.queryParams("inputTableName"));
            String outputTableName = URLParser.decodeNullableURLParam(request.queryParams("outputTableName"));
            String kvsMaster = URLParser.decodeNullableURLParam(request.queryParams("kvsMaster"));
            String fromKey = URLParser.decodeNullableURLParam(request.queryParams("fromKey"));
            String toKey = URLParser.decodeNullableURLParam(request.queryParams("toKey"));
            var lambda = (FlameRDD.StringToBoolean) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
            KVSClient kvs = new KVSClient(kvsMaster);
            KVSClient.KVSIterator rowIterator = kvs.scan(inputTableName, fromKey, toKey);
            while (rowIterator.hasNext()) {
                Row row = rowIterator.next();
                String value = row.get("value");
                if (lambda.op(value)) {
                    kvs.put(outputTableName, row.key(), "value", value);
                }
            }
            rowIterator.close();
            return "OK";
        });

        post("/rdd/mapPartitions", (request, response) -> {
            String inputTableName = URLParser.decodeNullableURLParam(request.queryParams("inputTableName"));
            String outputTableName = URLParser.decodeNullableURLParam(request.queryParams("outputTableName"));
            String kvsMaster = URLParser.decodeNullableURLParam(request.queryParams("kvsMaster"));
            String fromKey = URLParser.decodeNullableURLParam(request.queryParams("fromKey"));
            String toKey = URLParser.decodeNullableURLParam(request.queryParams("toKey"));
            var lambda = (FlameRDD.IteratorToIterator) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
            KVSClient kvs = new KVSClient(kvsMaster);
            Iterator<String> inputIterator = new Iterator<>() {
                final public KVSClient.KVSIterator rowIterator = kvs.scan(inputTableName, fromKey, toKey);

                @Override
                public boolean hasNext() {
                    return rowIterator.hasNext();
                }

                @Override
                public String next() {
                    return rowIterator.next().get("value");
                }
            };
            Iterator<String> outputIterator = lambda.op(inputIterator);
            while (outputIterator.hasNext()) {
                String output = outputIterator.next();
                kvs.put(outputTableName, generateRandomAlphabeticString(), "value", output);
            }
            KVSClient.KVSIterator.class.getMethod("close").invoke(
                    inputIterator.getClass().getField("rowIterator").get(inputIterator)
            );
            return "OK";
        });

        post("/rdd/cogroup-pair", (request, response) -> {
            String inputTableName = URLParser.decodeNullableURLParam(request.queryParams("inputTableName"));
            String outputTableName = URLParser.decodeNullableURLParam(request.queryParams("outputTableName"));
            String kvsMaster = URLParser.decodeNullableURLParam(request.queryParams("kvsMaster"));
            String fromKey = URLParser.decodeNullableURLParam(request.queryParams("fromKey"));
            String toKey = URLParser.decodeNullableURLParam(request.queryParams("toKey"));
            String otherTableName = URLParser.decodeNullableURLParam(request.queryParams("extra"));
            KVSClient kvs = new KVSClient(kvsMaster);
            {
                KVSClient.KVSIterator rowIterator = kvs.scan(inputTableName, fromKey, toKey);
                while (rowIterator.hasNext()) {
                    Row row = rowIterator.next();
                    StringBuilder stringBuilder = formatRowOutput(row);
                    kvs.put(outputTableName, row.key(), "value", stringBuilder.append(",[]").toString());
                }
                rowIterator.close();
            }
            {
                KVSClient.KVSIterator rowIterator = kvs.scan(otherTableName, fromKey, toKey);
                while (rowIterator.hasNext()) {
                    Row row = rowIterator.next();
                    String key = row.key();
                    StringBuilder stringBuilder = formatRowOutput(row);
                    Row existingRow = kvs.getRow(outputTableName, key);
                    if (existingRow != null) {
                        String existingValue = existingRow.get("value");
                        kvs.put(outputTableName, key, "value",
                                existingValue.substring(0, existingValue.length() - 2) + stringBuilder);
                    } else {
                        kvs.put(outputTableName, key, "value", "[]," + stringBuilder);
                    }
                }
                rowIterator.close();
            }
            return "OK";
        });
    }

    private static StringBuilder formatRowOutput(Row row) {
        StringBuilder stringBuilder = new StringBuilder("[");
        boolean isFirstIter = true;
        for (String column : row.columns()) {
            if (isFirstIter) {
                isFirstIter = false;
            } else {
                stringBuilder.append(",");
            }
            stringBuilder.append(row.get(column));
        }
        stringBuilder.append("]");
        return stringBuilder;
    }
}
