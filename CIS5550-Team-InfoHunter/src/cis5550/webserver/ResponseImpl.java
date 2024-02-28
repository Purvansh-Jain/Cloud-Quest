package cis5550.webserver;

import cis5550.tools.Logger;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ResponseImpl implements Response {
    private static final Logger LOGGER = Logger.getLogger(Server.class);

    private final boolean isConnectionSecure;

    private OutputStream outputStream;

    private int statusCode = 200;

    private String reasonPhrase = "OK";

    public boolean isConnectionSecure() {
        return isConnectionSecure;
    }

    public boolean isHandled() {
        return isHandled;
    }

    public void setHandled(boolean handled) {
        isHandled = handled;
    }

    private boolean isHandled = false;

    public Map<String, List<String>> getHeaders() {
        return headers;
    }

    private Map<String, List<String>> headers = new HashMap<>();

    private InputStream bodyInputStream = null;

    public void setShouldIgnoreBody(boolean shouldIgnoreBody) {
        this.shouldIgnoreBody = shouldIgnoreBody;
    }

    private boolean shouldIgnoreBody = false;

    public boolean isWritten() {
        return isWritten;
    }

    private boolean isWritten = false;

    public ResponseImpl(OutputStream outputStream, boolean isConnectionSecure) {
        this.outputStream = outputStream;
        this.isConnectionSecure = isConnectionSecure;
    }

    @Override
    public void body(String body) {
        byte[] rawBody = body.getBytes(StandardCharsets.UTF_8);
        bodyInputStream = new ByteArrayInputStream(rawBody);
        headers.put("content-length", List.of(String.valueOf(rawBody.length)));
    }

    @Override
    public void bodyAsBytes(byte[] bodyArg) {
        bodyInputStream = new ByteArrayInputStream(bodyArg);
        headers.put("content-length", List.of(String.valueOf(bodyArg.length)));
    }

    public void bodyAsInputStream(InputStream bodyInputStream) {
        this.bodyInputStream = bodyInputStream;
    }

    @Override
    public void header(String name, String value) {
        if (isWritten) {
            return;
        }
        headers.computeIfAbsent(name, k -> new ArrayList<>()).add(value);
    }

    public void headerOverwrite(String name, String value) {
        if (isWritten) {
            return;
        }
        headers.put(name, List.of(value));
    }

    @Override
    public void type(String contentType) {
        if (isWritten) {
            return;
        }
        header("content-type", contentType);
    }

    @Override
    public void status(int statusCode, String reasonPhrase) {
        if (isWritten) {
            return;
        }
        this.statusCode = statusCode;
        this.reasonPhrase = reasonPhrase;
    }

    private void writeHeaders() throws IOException {
        StringBuilder responseHeaderBuilder = new StringBuilder();
        responseHeaderBuilder.append("HTTP/1.1 ").append(statusCode).append(' ').append(reasonPhrase).append("\r\n");
        for (var entry : headers.entrySet()) {
            for (var value : entry.getValue()) {
                responseHeaderBuilder.append(entry.getKey()).append(": ").append(value).append("\r\n");
            }
        }
        responseHeaderBuilder.append("\r\n");
        String responseHeader = responseHeaderBuilder.toString();
        outputStream.write(responseHeader.getBytes());
        LOGGER.debug("Sent response header:\n" + responseHeader);
    }

    @Override
    public void write(byte[] b) throws Exception {
        if (!isWritten) {
            headers.put("connection", List.of("close"));
            writeHeaders();
            isWritten = true;
        }
        outputStream.write(b);
    }

    public void writeDefault() throws IOException {
        if (isWritten) {
            return;
        }
        isWritten = true;
        if (bodyInputStream == null) {
            headers.putIfAbsent("content-length", List.of("0"));
        }
        writeHeaders();
        if (!shouldIgnoreBody && bodyInputStream != null) {
            bodyInputStream.transferTo(outputStream);
            bodyInputStream.close();
        }
    }

    @Override
    public void redirect(String url, int responseCode) {
        final Map<Integer, String> redirectStatusCodes = Map.of(
                301, "Moved Permanently",
                302, "Found",
                303, "See Other",
                307, "Temporary Redirect",
                308, "Permanent Redirect");
        status(responseCode, redirectStatusCodes.get(responseCode));
        header("Location", url);
    }

    @Override
    public void halt(int statusCode, String reasonPhrase) {
        status(statusCode, reasonPhrase);
        isHandled = true;
    }
}
