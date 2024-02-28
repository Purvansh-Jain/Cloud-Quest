package cis5550.flame;

import cis5550.kvs.KVSClient;
import cis5550.tools.HTTP;
import cis5550.tools.Partitioner;
import cis5550.tools.Serializer;
import cis5550.tools.StringUtil;

import java.io.IOException;
import java.io.Serializable;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

import static cis5550.tools.StringUtil.generateRandomAlphabeticString;

public class FlameContextImpl implements FlameContext, Serializable {
    private final StringBuilder outputBuilder = new StringBuilder();

    private boolean outputCalled = false;

    public FlameContextImpl(String jarName) {

    }

    @Override
    public KVSClient getKVS() {
        return Master.kvs;
    }

    @Override
    public void output(String s) {
        outputBuilder.append(s);
        outputCalled = true;
    }

    @Override
    public FlameRDD parallelize(List<String> list) throws Exception {
        String tableName = generateRandomAlphabeticString();
        for (String value : list) {
            Master.kvs.put(tableName, generateRandomAlphabeticString(), "value", value);
        }
        return new FlameRDDImpl(tableName);
    }


    // client must make sure that tableName has not been declared, or else it will be overwritten
    @Override
    public FlameRDD parallelize(List<String> list, String tableName) throws Exception {
        if (tableName == null) {
            tableName = generateRandomAlphabeticString();
        }

        for (String value : list) {
            Master.kvs.put(tableName, generateRandomAlphabeticString(), "value", value); // or use Hasher.hash url code
        }
        return new FlameRDDImpl(tableName);
    }


    @Override
    public FlameRDD fromTable(String tableName, RowToString lambda) throws Exception {
        return new FlameRDDImpl(
                FlameContextImpl.invokeOperation(tableName, "/rdd/fromTable", Serializer.objectToByteArray(lambda), null)
        );
    }

    @Override
    public void setConcurrencyLevel(int keyRangesPerWorker) {

    }

    public String getOutput() {
        if (outputCalled) {
            return outputBuilder.toString();
        } else {
            return "No output available";
        }
    }

    static public String invokeOperation(String inputTableName, String operationName, byte[] lambda, String extra, String tableName) throws Exception {
        String outputTableName = tableName == null ? generateRandomAlphabeticString() : tableName;
        List<Partitioner.Partition> partitions = new ArrayList<>();
        List<String> flameWorkers = Master.getWorkers();
        for (int i = 0; i < Master.kvs.numWorkers(); i++) {
            String kvsWorkerAddress = Master.kvs.getWorkerAddress(i),
                    startRow = Master.kvs.getWorkerID(i),
                    endRowExclusive = i < Master.kvs.numWorkers() - 1 ? Master.kvs.getWorkerID(i + 1) : Master.kvs.getWorkerID(0);
            {
                int maxPrefixLength = 0;
                String preferredFlameWorker = null;
                for (String flameWorker : flameWorkers) {
                    int prefixLength = StringUtil.getCommonPrefixLength(flameWorker, kvsWorkerAddress);
                    if (prefixLength > maxPrefixLength) {
                        maxPrefixLength = prefixLength;
                        preferredFlameWorker = flameWorker;
                    }
                }
                if (preferredFlameWorker == null) {
                    preferredFlameWorker = flameWorkers.get(ThreadLocalRandom.current().nextInt(flameWorkers.size()));
                }
                partitions.add(new Partitioner.Partition(kvsWorkerAddress, startRow, endRowExclusive, preferredFlameWorker));
            }
        }

        List<Thread> threads = new ArrayList<>();
        AtomicBoolean hasFailed = new AtomicBoolean(false);
        for (Partitioner.Partition partition : partitions) {
            System.out.println("Assigned partition: " + partition);
            String flameWorker = partition.assignedFlameWorker;
            String fromKey = partition.fromKey;
            String toKey = partition.toKeyExclusive;
            String url = "http://" + flameWorker + operationName +
                    "?inputTableName=" + URLEncoder.encode(inputTableName, StandardCharsets.UTF_8) +
                    "&outputTableName=" + URLEncoder.encode(outputTableName, StandardCharsets.UTF_8) +
                    "&kvsMaster=" + URLEncoder.encode(Master.kvs.getMaster(), StandardCharsets.UTF_8) +
                    (fromKey != null ? "&fromKey=" + URLEncoder.encode(fromKey, StandardCharsets.UTF_8) : "") +
                    (toKey != null ? "&toKey=" + URLEncoder.encode(toKey, StandardCharsets.UTF_8) : "") +
                    (extra != null ? "&extra=" + URLEncoder.encode(extra, StandardCharsets.UTF_8) : "");
            Thread thread = new Thread(() -> {
                try {
                    System.out.println("Sending request to: " + url);
                    HTTP.Response response = HTTP.doRequest("POST", url, lambda);
                    if (response.statusCode() != 200) {
                        throw new IOException("Received erroneous status code: " + response.statusCode());
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                    System.out.println(e.getMessage());
                    hasFailed.set(true);
                }
            });
            thread.start();
            threads.add(thread);
        }

        for (Thread thread : threads) {
            try {
                thread.join();
                System.out.println("Joined thread: " + thread.getName());
            } catch (InterruptedException e) {
                e.printStackTrace();
                hasFailed.set(true);
            }
        }
        if (hasFailed.get()) {
            throw new Exception("One or more threads have failed");
        }
        return outputTableName;
    }

    static public String invokeOperation(String inputTableName, String operationName, byte[] lambda, String extra) throws Exception {
        return invokeOperation(inputTableName, operationName, lambda, extra, null);
    }

    // a fork of invokeOperation that does not generate a new output table name but instead returns the responses
    static public List<String> invokeOperationForResponses(String inputTableName, String operationName, byte[] lambda, String extra) throws Exception {
        List<Partitioner.Partition> partitions = new ArrayList<>();
        List<String> flameWorkers = Master.getWorkers();
        for (int i = 0; i < Master.kvs.numWorkers(); i++) {
            String kvsWorkerAddress = Master.kvs.getWorkerAddress(i),
                    startRow = Master.kvs.getWorkerID(i),
                    endRowExclusive = i < Master.kvs.numWorkers() - 1 ? Master.kvs.getWorkerID(i + 1) : Master.kvs.getWorkerID(0);
            {
                int maxPrefixLength = 0;
                String preferredFlameWorker = null;
                for (String flameWorker : flameWorkers) {
                    int prefixLength = StringUtil.getCommonPrefixLength(flameWorker, kvsWorkerAddress);
                    if (prefixLength > maxPrefixLength) {
                        maxPrefixLength = prefixLength;
                        preferredFlameWorker = flameWorker;
                    }
                }
                if (preferredFlameWorker == null) {
                    preferredFlameWorker = flameWorkers.get(ThreadLocalRandom.current().nextInt(flameWorkers.size()));
                }
                partitions.add(new Partitioner.Partition(kvsWorkerAddress, startRow, endRowExclusive, preferredFlameWorker));
            }
        }

        List<Thread> threads = new ArrayList<>();
        List<String> responses = Collections.synchronizedList(new ArrayList<>());
        AtomicBoolean hasFailed = new AtomicBoolean(false);
        for (Partitioner.Partition partition : partitions) {
            String flameWorker = partition.assignedFlameWorker;
            String fromKey = partition.fromKey;
            String toKey = partition.toKeyExclusive;
            String url = "http://" + flameWorker + operationName +
                    "?inputTableName=" + inputTableName +
                    "&kvsMaster=" + Master.kvs.getMaster() +
                    (fromKey != null ? "&fromKey=" + fromKey : "") +
                    (toKey != null ? "&toKey=" + toKey : "") +
                    (extra != null ? "&extra=" + URLEncoder.encode(extra, StandardCharsets.UTF_8) : "");
            Thread thread = new Thread(() -> {
                try {
                    HTTP.Response response = HTTP.doRequest("POST", url, lambda);
                    if (response.statusCode() != 200) {
                        throw new IOException("Received erroneous status code: " + response.statusCode() + ", message: " + Arrays.toString(response.body()));
                    }
                    responses.add(new String(response.body(), StandardCharsets.UTF_8));
                } catch (IOException e) {
                    System.out.println(e.getMessage());
                    e.printStackTrace();
                    hasFailed.set(true);
                }
            });
            thread.start();
            threads.add(thread);
        }

        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                System.out.println(e.getMessage());
                e.printStackTrace();
                hasFailed.set(true);
            }
        }
        if (hasFailed.get()) {
            throw new Exception("One or more threads have failed");
        }
        return responses;
    }
}
