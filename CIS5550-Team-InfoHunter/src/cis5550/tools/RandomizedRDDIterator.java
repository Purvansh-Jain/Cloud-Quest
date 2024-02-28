package cis5550.tools;

import cis5550.kvs.KVSClient;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

/**
 * An iterator that returns the values in a RDD in a locally randomized order.
 */
public class RandomizedRDDIterator implements Iterator<String> {
    private final KVSClient.KVSIterator baseIterator;

    private final List<String> buffer;

    private final Random random = new Random();

    public RandomizedRDDIterator(KVSClient.KVSIterator baseIterator) {
        this(baseIterator, 65536);
    }

    public RandomizedRDDIterator(KVSClient.KVSIterator baseIterator, int bufferSize) {
        if (bufferSize < 1) {
            throw new IllegalArgumentException("Buffer size must be at least 1");
        }
        this.baseIterator = baseIterator;
        this.buffer = new ArrayList<>(bufferSize);
        while (baseIterator.hasNext() && buffer.size() < bufferSize) {
            buffer.add(getNextValue());
        }
    }

    @Override
    public boolean hasNext() {
        return !buffer.isEmpty();
    }

    @Override
    public String next() {
        int index = random.nextInt(buffer.size());
        String result = buffer.get(index);
        if (baseIterator.hasNext()) {
            buffer.set(index, getNextValue());
        } else {
            String last = buffer.remove(buffer.size() - 1);
            if (index < buffer.size()) {
                buffer.set(index, last);
            }
        }
        return result;
    }

    private String getNextValue() {
        return baseIterator.next().get("value");
    }
}
