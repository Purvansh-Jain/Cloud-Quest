package cis5550.flame;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Serializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

public class FlameRDDImpl implements FlameRDD {
    public String tableName;

    public FlameRDDImpl(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public int count() throws Exception {
        return Master.kvs.count(tableName);
    }

    @Override
    public void saveAsTable(String tableNameArg) throws Exception {
        Master.kvs.rename(tableName, tableNameArg);
        tableName = tableNameArg;
    }

    @Override
    public FlameRDD distinct() throws Exception {
        return new FlameRDDImpl(
                FlameContextImpl.invokeOperation(tableName, "/rdd/distinct", null, null)
        );
    }

    @Override
    public Vector<String> take(int num) throws Exception {
        KVSClient.KVSIterator rowIterator = Master.kvs.scan(tableName);
        Vector<String> result = new Vector<>();
        for (int i = 0; i < num && rowIterator.hasNext(); i++) {
            Row row = rowIterator.next();
            result.add(row.get("value"));
        }
        rowIterator.close();
        return result;
    }

    @Override
    public String fold(String zeroElement, FlamePairRDD.TwoStringsToString lambda) throws Exception {
        List<String> responses = FlameContextImpl.invokeOperationForResponses(tableName, "/rdd/fold",
                Serializer.objectToByteArray(lambda), zeroElement);
        String accumulator = zeroElement;
        for (String response : responses) {
            accumulator = lambda.op(accumulator, response);
        }
        return accumulator;
    }

    @Override
    public List<String> collect() throws Exception {
        List<String> result = new ArrayList<>();
        KVSClient.KVSIterator rowIterator = Master.kvs.scan(tableName);
        while (rowIterator.hasNext()) {
            Row row = rowIterator.next();
            result.add(row.get("value"));
        }
        rowIterator.close();
        return result;
    }

    @Override
    public FlameRDD flatMap(StringToIterable lambda) throws Exception {
        return new FlameRDDImpl(
                FlameContextImpl.invokeOperation(tableName, "/rdd/flatMap", Serializer.objectToByteArray(lambda), null)
        );
    }

    @Override
    public FlameRDD flatMap(StringToIterable lambda, String persistTable) throws Exception {
        Master.kvs.persist(persistTable);
        return new FlameRDDImpl(
                FlameContextImpl.invokeOperation(tableName, "/rdd/flatMap", Serializer.objectToByteArray(lambda), "hash", persistTable)
        );
    }

    @Override
    public FlameRDD flatMapRandom(StringToIterable lambda, String persistTable) throws Exception {
        Master.kvs.persist(persistTable);
        return new FlameRDDImpl(
                FlameContextImpl.invokeOperation(tableName, "/rdd/flatMap-random", Serializer.objectToByteArray(lambda), "hash", persistTable)
        );
    }

    @Override
    public FlamePairRDD flatMapToPair(StringToPairIterable lambda) throws Exception {
        return new FlamePairRDDImpl(
                FlameContextImpl.invokeOperation(tableName, "/rdd/flatMapToPair", Serializer.objectToByteArray(lambda), null)
        );
    }

    @Override
    public FlamePairRDD flatMapToPair(StringToPairIterable lambda, String persistTable) throws Exception {
        return new FlamePairRDDImpl(
                FlameContextImpl.invokeOperation(tableName, "/rdd/flatMapToPair", Serializer.objectToByteArray(lambda), null, persistTable)
        );
    }

    @Override
    public FlamePairRDD mapToPair(StringToPair lambda) throws Exception {
        return new FlamePairRDDImpl(
                FlameContextImpl.invokeOperation(tableName, "/rdd/mapToPair", Serializer.objectToByteArray(lambda), null)
        );
    }

    @Override
    public FlameRDD intersection(FlameRDD r) throws Exception {
        FlameRDDImpl flameRDDImpl = (FlameRDDImpl) r;
        return new FlameRDDImpl(
                FlameContextImpl.invokeOperation(tableName, "/rdd/intersection", null,
                        flameRDDImpl != null ? flameRDDImpl.tableName : null)
        );
    }

    @Override
    public FlameRDD sample(double f) throws Exception {
        return new FlameRDDImpl(
                FlameContextImpl.invokeOperation(tableName, "/rdd/sample", null, String.valueOf(f))
        );
    }

    @Override
    public FlamePairRDD groupBy(StringToString lambda) throws Exception {
        return new FlamePairRDDImpl(
                FlameContextImpl.invokeOperation(tableName, "/rdd/groupBy", Serializer.objectToByteArray(lambda), null)
        );
    }

    @Override
    public FlameRDD filter(StringToBoolean lambda) throws Exception {
        return new FlameRDDImpl(
                FlameContextImpl.invokeOperation(tableName, "/rdd/filter", Serializer.objectToByteArray(lambda), null)
        );
    }

    @Override
    public FlameRDD mapPartitions(IteratorToIterator lambda) throws Exception {
        return new FlameRDDImpl(
                FlameContextImpl.invokeOperation(tableName, "/rdd/mapPartitions", Serializer.objectToByteArray(lambda), null)
        );
    }
}
