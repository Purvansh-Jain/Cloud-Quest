package cis5550.kvs;

import java.util.*;
import java.io.*;
import java.util.function.BiFunction;

public class Row implements Serializable {

    protected String key;
    protected HashMap<String, byte[]> values;

    public Row(String keyArg) {
        key = keyArg;
        values = new HashMap<>(2);
    }

    public synchronized String key() {
        return key;
    }

    public synchronized Row clone() {
        Row theClone = new Row(key);
        for (String s : values.keySet()) {
            theClone.values.put(s, values.get(s).clone());
        }
        return theClone;
    }

    public synchronized Set<String> columns() {
        return values.keySet();
    }

    public synchronized void put(String key, String value) {
        values.put(key, value.getBytes());
    }

    public synchronized void put(String key, byte[] value) {
        values.put(key, value);
    }

    public synchronized byte[] compute(String key, BiFunction<String, String, String> mapper) {
        return values.compute(key, (k, v) -> {
            if (v == null) {
                return mapper.apply(k, null).getBytes();
            } else {
                return mapper.apply(k, new String(v)).getBytes();
            }
        });
    }

    public synchronized String get(String key) {
        byte[] bytes = values.get(key);
        if (bytes == null) {
            return null;
        }
        return new String(bytes);
    }

    public synchronized byte[] getBytes(String key) {
        return values.get(key);
    }

    static String readStringSpace(InputStream in) throws Exception {
        byte buffer[] = new byte[1024];
        int numRead = 0;
        while (true) {
            if (numRead == buffer.length) {
                throw new Exception("Format error: Expecting string+space");
            }

            int b = in.read();
            if ((b < 0) || (b == 10)) {
                return null;
            }
            buffer[numRead++] = (byte) b;
            if (b == ' ') {
                return new String(buffer, 0, numRead - 1);
            }
        }
    }

    static String readStringSpace(RandomAccessFile in) throws Exception {
        byte buffer[] = new byte[1024];
        int numRead = 0;
        while (true) {
            if (numRead == buffer.length) {
                throw new Exception("Format error: Expecting string+space");
            }

            int b = in.read();
            if ((b < 0) || (b == 10)) {
                return null;
            }
            buffer[numRead++] = (byte) b;
            if (b == ' ') {
                return new String(buffer, 0, numRead - 1);
            }
        }
    }

    public static Row readFrom(InputStream in) throws Exception {
        String theKey = readStringSpace(in);
        if (theKey == null) {
            return null;
        }

        Row newRow = new Row(theKey);
        while (true) {
            String keyOrMarker = readStringSpace(in);
            if (keyOrMarker == null) {
                return newRow;
            }

            int len = Integer.parseInt(readStringSpace(in));
            byte[] theValue = new byte[len];
            int bytesRead = 0;
            while (bytesRead < len) {
                int n = in.read(theValue, bytesRead, len - bytesRead);
                if (n < 0) {
                    throw new Exception("Premature end of stream while reading value for key '" + keyOrMarker + "' (read " + bytesRead + " bytes, expecting " + len + ")");
                }
                bytesRead += n;
            }

            byte b = (byte) in.read();
            if (b != ' ') {
                throw new Exception("Expecting a space separator after value for key '" + keyOrMarker + "'");
            }

            newRow.put(keyOrMarker, theValue);
        }
    }

    public static Row readFrom(RandomAccessFile in) throws Exception {
        String theKey = readStringSpace(in);
        if (theKey == null) {
            return null;
        }

        Row newRow = new Row(theKey);
        while (true) {
            String keyOrMarker = readStringSpace(in);
            if (keyOrMarker == null) {
                return newRow;
            }

            int len = Integer.parseInt(readStringSpace(in));
            byte[] theValue = new byte[len];
            int bytesRead = 0;
            while (bytesRead < len) {
                int n = in.read(theValue, bytesRead, len - bytesRead);
                if (n < 0) {
                    throw new Exception("Premature end of stream while reading value for key '" + keyOrMarker + "' (read " + bytesRead + " bytes, expecting " + len + ")");
                }
                bytesRead += n;
            }

            byte b = (byte) in.read();
            if (b != ' ') {
                throw new Exception("Expecting a space separator after value for key '" + keyOrMarker + "'");
            }

            newRow.put(keyOrMarker, theValue);
        }
    }

    public synchronized String toString() {
        String s = key + " {";
        boolean isFirst = true;
        for (String k : values.keySet()) {
            byte[] value = values.get(k);
            s = s + (isFirst ? " " : ", ") + k + ": " + new String(value);
            isFirst = false;
        }
        return s + " }";
    }

    public synchronized byte[] toByteArray() {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try {
            baos.write(key.getBytes());
            baos.write(' ');

            for (String s : values.keySet()) {
                byte[] value = values.get(s);
                baos.write(s.getBytes());
                baos.write(' ');
                baos.write(("" + value.length).getBytes());
                baos.write(' ');
                baos.write(value);
                baos.write(' ');
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("This should not happen!");
        }

        return baos.toByteArray();
    }
}