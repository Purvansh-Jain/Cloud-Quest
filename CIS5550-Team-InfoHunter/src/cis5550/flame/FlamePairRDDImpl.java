package cis5550.flame;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Serializer;

import java.util.ArrayList;
import java.util.List;

public class FlamePairRDDImpl implements FlamePairRDD {
    public String tableName;

    public FlamePairRDDImpl(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public List<FlamePair> collect() throws Exception {
        List<FlamePair> result = new ArrayList<>();
        KVSClient.KVSIterator rowIterator = Master.kvs.scan(tableName);
        while (rowIterator.hasNext()) {
            Row row = rowIterator.next();
            String rowKey = row.key();
            for (String column : row.columns()) {
                result.add(new FlamePair(rowKey, row.get(column)));
            }
        }
        rowIterator.close();
        return result;
    }

    @Override
    public FlamePairRDD foldByKey(String zeroElement, TwoStringsToString lambda) throws Exception {
        return new FlamePairRDDImpl(
                FlameContextImpl.invokeOperation(tableName, "/rdd/foldByKey-pair", Serializer.objectToByteArray(lambda), zeroElement)
        );
    }

    @Override
    public void saveAsTable(String tableNameArg) throws Exception {
        Master.kvs.rename(tableName, tableNameArg);
        tableName = tableNameArg;
    }

    @Override
    public FlameRDD flatMap(PairToStringIterable lambda) throws Exception {
        return new FlameRDDImpl(
                FlameContextImpl.invokeOperation(tableName, "/rdd/flatMap-pair", Serializer.objectToByteArray(lambda), null)
        );
    }

    @Override
    public FlameRDD flatMap(PairToStringIterable lambda, String persistTable) throws Exception {
        return new FlameRDDImpl(
                FlameContextImpl.invokeOperation(tableName, "/rdd/flatMap-pair", Serializer.objectToByteArray(lambda), null, persistTable)
        );
    }

    @Override
    public FlamePairRDD flatMapToPair(PairToPairIterable lambda) throws Exception {
        return new FlamePairRDDImpl(
                FlameContextImpl.invokeOperation(tableName, "/rdd/flatMapToPair-pair", Serializer.objectToByteArray(lambda), null)
        );
    }

    @Override
    public FlamePairRDD flatMapToPair(PairToPairIterable lambda, String persistTable) throws Exception {
        return new FlamePairRDDImpl(
                FlameContextImpl.invokeOperation(tableName, "/rdd/flatMapToPair-pair", Serializer.objectToByteArray(lambda), null, persistTable)
        );
    }

    @Override
    public FlamePairRDD join(FlamePairRDD other) throws Exception {
        FlamePairRDDImpl flamePairRDDImpl = (FlamePairRDDImpl) other;
        return new FlamePairRDDImpl(
                FlameContextImpl.invokeOperation(tableName, "/rdd/join-pair", null,
                        flamePairRDDImpl != null ? flamePairRDDImpl.tableName : null)
        );
    }

    @Override
    public FlamePairRDD cogroup(FlamePairRDD other) throws Exception {
        return new FlamePairRDDImpl(
                FlameContextImpl.invokeOperation(tableName, "/rdd/cogroup-pair", null, ((FlamePairRDDImpl) other).tableName)
        );
    }


    public int count() throws Exception {
        return this.collect().size();
    }
}
