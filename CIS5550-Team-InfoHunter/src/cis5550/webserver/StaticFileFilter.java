package cis5550.webserver;

import java.io.ByteArrayOutputStream;
import java.io.RandomAccessFile;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

public class StaticFileFilter implements Filter {
    private static final Map<String, String> MIME_TYPES = Map.of(
            "html", "text/html",
            "htm", "text/html",
            "txt", "text/plain",
            "jpg", "image/jpeg",
            "jpeg", "image/jpeg"
    );

    private static final String MULTIPART_BOUNDARY = "t47gcindx1uxrawcbzxj";

    @Override
    public void handle(Request request, Response response) throws Exception {
        if (!(response instanceof ResponseImpl responseImpl)) {
            response.status(500, "Response is not an instance of ResponseImpl");
            return;
        }
        if (responseImpl.isHandled()) {
            return;
        }
        responseImpl.headerOverwrite("content-type", "application/octet-stream");
        responseImpl.headerOverwrite("content-length", "0");
        response.body("");
        if (request.url().contains("..")) {
            responseImpl.status(403, "Forbidden");
            return;
        }
        if (Server.staticFiles.loc == null) {
            responseImpl.status(404, "Not Found");
            return;
        }
        Path tempPath = Path.of(Server.staticFiles.loc, request.url());
        if (!Files.isRegularFile(tempPath)) {
            responseImpl.status(404, "Not Found");
            return;
        }
        if(!Files.isReadable(tempPath)){
            responseImpl.status(403, "Forbidden");
            return;
        }
        if (request.headers().contains("if-modified-since")) {
            Instant lastModifiedTime = Files.getLastModifiedTime(tempPath).toInstant();
            Instant ifModifiedSince = ZonedDateTime.parse(
                    request.headers("if-modified-since"),
                    DateTimeFormatter.RFC_1123_DATE_TIME).toInstant();
            if (lastModifiedTime.isBefore(ifModifiedSince)) {
                responseImpl.status(304, "Not Modified");
                return;
            }
        }
        long fileSize = Files.size(tempPath);
        responseImpl.headerOverwrite("content-length", String.valueOf(fileSize));
        String extension = request.url().substring(request.url().lastIndexOf('.') + 1);
        if (MIME_TYPES.containsKey(extension)) {
            responseImpl.headerOverwrite("content-type", MIME_TYPES.get(extension));
        }
        if (request.headers().contains("range")) {
            ByteArrayOutputStream bufferStream = new ByteArrayOutputStream();
            String rangeStr = request.headers("range");
            if (rangeStr.startsWith("bytes=")) {
                rangeStr = rangeStr.substring(6);
            }
            String[] ranges = rangeStr.split(",\\s*");
            String fileSizeStr = String.valueOf(fileSize);
            try (RandomAccessFile file = new RandomAccessFile(tempPath.toFile(), "r")) {
                if (ranges.length == 1) {
                    String[] bounds = ranges[0].split("-");
                    long start = Long.parseLong(bounds[0]);
                    long end = Long.parseLong(bounds[1]);
                    file.getChannel().transferTo(start, end - start + 1, Channels.newChannel(bufferStream));
                    responseImpl.headerOverwrite("content-range", "bytes " + start + "-" + end + "/" + fileSizeStr);
                } else {
                    for (String range : ranges) {
                        bufferStream.write(("--" + MULTIPART_BOUNDARY + "\r\n").getBytes());
                        String[] bounds = range.split("-");
                        long start = Long.parseLong(bounds[0]);
                        long end = Long.parseLong(bounds[1]);
                        file.getChannel().transferTo(start, end - start + 1, Channels.newChannel(bufferStream));
                        bufferStream.write("\r\n".getBytes());
                    }
                    bufferStream.write(("--" + MULTIPART_BOUNDARY + "--\r\n").getBytes());
                    responseImpl.headerOverwrite("content-type", "multipart/byteranges; boundary=" + MULTIPART_BOUNDARY);
                }
            }
            responseImpl.headerOverwrite("content-length", String.valueOf(bufferStream.size()));
            responseImpl.bodyAsBytes(bufferStream.toByteArray());
        } else {
            responseImpl.bodyAsInputStream(Files.newInputStream(tempPath));
        }
        responseImpl.setHandled(true);
    }
}
