package cis5550.webserver;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class SessionImpl implements Session {
    private final static int DEFAULT_MAX_ACTIVE_INTERVAL = 300;

    private final String id;

    private final long creationTime;

    private final AtomicLong lastAccessedTime;

    private final AtomicInteger maxActiveInterval;

    private final AtomicBoolean isValid;

    private final Map<String, Object> attributes = new ConcurrentHashMap<>();

    public SessionImpl(String id, long now) {
        this.id = id;
        this.creationTime = now;
        this.lastAccessedTime = new AtomicLong(now);
        this.maxActiveInterval = new AtomicInteger(DEFAULT_MAX_ACTIVE_INTERVAL);
        this.isValid = new AtomicBoolean(true);
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public long creationTime() {
        return creationTime;
    }

    @Override
    public long lastAccessedTime() {
        return lastAccessedTime.get();
    }

    public void lastAccessedTime(long time) {
        lastAccessedTime.set(time);
    }

    public int maxActiveInterval() {
        return maxActiveInterval.get();
    }

    @Override
    public void maxActiveInterval(int seconds) {
        maxActiveInterval.set(seconds);
    }

    public boolean getIsValid() {
        return isValid.get();
    }

    @Override
    public void invalidate() {
        isValid.set(false);
    }

    @Override
    public Object attribute(String name) {
        return attributes.get(name);
    }

    @Override
    public void attribute(String name, Object value) {
        attributes.put(name, value);
    }
}
