package cis5550.webserver;

import cis5550.tools.Logger;
import cis5550.tools.SNIInspector;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLContext;
import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;

public class Server {

    public static class staticFiles {
        public static String loc = null;

        public static void location(String s) {
            staticFiles.loc = s;
        }
    }

    private static final Filter STATIC_FILE_FILTER = new StaticFileFilter();

    private static final Filter SESSION_UPDATE_FILTER = new SessionUpdateFilter();

    private static final Map<String, Map<String, List<Map.Entry<String, Route>>>> ROUTE_TABLE = new HashMap<>();

    private static final List<Filter> PRE_FILTERS = new ArrayList<>(), POST_FILTERS = new ArrayList<>();

    public static final Map<String, Session> SESSIONS = new ConcurrentHashMap<>();

    public static final Map<String, String> KEY_STORES = new HashMap<>();

    public static final Map<String, String> PASSWORDS = new HashMap<>();

    private static int port = 80;

    private static int securePort = -1;

    private static Server instance = null;

    private static boolean running = false;

    private static String activeHost = "";

    private static String readline(InputStream inputStream) throws IOException {
        StringBuilder sb = new StringBuilder();
        int last, secondLast = -1;
        while ((last = inputStream.read()) != -1) {
            if (secondLast == '\r' && last == '\n') {
                return sb.toString();
            }
            if (secondLast != -1) {
                sb.append((char) secondLast);
            }
            secondLast = last;
        }
        return null;
    }

    private static void init() {
        if (instance == null) {
            instance = new Server();
            PRE_FILTERS.add((request, response) -> {
                if (!request.protocol().equals("HTTP/1.1")) {
                    response.halt(505, "HTTP Version Not Supported");
                    return;
                }
                response.header("server", "CIS 555 Web Server (Xinyue Liu)");
                response.header("accept-ranges", "bytes");
            });
            POST_FILTERS.add(STATIC_FILE_FILTER);
            POST_FILTERS.add(SESSION_UPDATE_FILTER);
        }
        if (!running) {
            new Thread(() -> {
                try {
                    ServerSocket acceptSocket = new ServerSocket(port);
                    instance.runServerLoop(acceptSocket, false);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }).start();
            if (securePort != -1) {
                new Thread(() -> {
                    try {
                        ServerSocket acceptSocket = new ServerSocket(securePort);
                        instance.runServerLoop(acceptSocket, true);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }).start();
            }
            new Thread(() -> {
                try {
                    while (true) {
                        Thread.sleep(1000 * 60 * 10);
                        long now = System.currentTimeMillis();
                        SESSIONS.entrySet().removeIf(
                                entry -> {
                                    SessionImpl session = (SessionImpl) entry.getValue();
                                    return now - session.lastAccessedTime() >= session.maxActiveInterval() * 1000L;
                                }
                        );
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }).start();
            running = true;
        }
    }

    public static void host(String host, String keyStoreFile, String password) {
        activeHost = host.split(":")[0].toLowerCase();
        KEY_STORES.put(activeHost, keyStoreFile);
        PASSWORDS.put(activeHost, password);
    }

    public static void port(int port) {
        Server.port = port;
    }

    public static void securePort(int port) {
        Server.securePort = port;
    }

    public static void get(String path, Route route) {
        init();
        ROUTE_TABLE.computeIfAbsent(activeHost, k -> new HashMap<>())
                .computeIfAbsent("GET", k -> new ArrayList<>())
                .add(Map.entry(path, route));
    }

    public static void post(String path, Route route) {
        init();
        ROUTE_TABLE.computeIfAbsent(activeHost, k -> new HashMap<>())
                .computeIfAbsent("POST", k -> new ArrayList<>())
                .add(Map.entry(path, route));
    }

    public static void put(String path, Route route) {
        init();
        ROUTE_TABLE.computeIfAbsent(activeHost, k -> new HashMap<>())
                .computeIfAbsent("PUT", k -> new ArrayList<>())
                .add(Map.entry(path, route));
    }

    public static Session getSession(String sessionId) {
        return SESSIONS.get(sessionId);
    }

    public static void addSession(String sessionId, Session session) {
        SESSIONS.put(sessionId, session);
    }

    private void runServerLoop(ServerSocket acceptSocket, boolean isConnectionSecure) {
        ExecutorService threadPool = Executors.newCachedThreadPool();
        while (true) {
            try {
                final Socket socket = isConnectionSecure ? getSecureSocket(acceptSocket.accept()) : acceptSocket.accept();
                threadPool.submit(() -> {
                    try {
                        while (true) {
                            ResponseImpl response = new ResponseImpl(socket.getOutputStream(), isConnectionSecure);
                            try {
                                // System.out.println("Started receiving data from socket " + socket);
                                InputStream inputStream = new BufferedInputStream(socket.getInputStream());
                                String headline = readline(inputStream);
                                if (headline == null) {
                                    socket.close();
                                    return;
                                }
                                // System.out.println(headline);
                                String[] headlineParts = headline.split(" ");
                                if (headlineParts.length != 3) {
                                    throw new IOException("400 Invalid Request: Unable to parse headline");
                                }
                                String method = headlineParts[0], path = headlineParts[1], protocol = headlineParts[2];
                                String line;
                                HashMap<String, String> requestHeaders = new HashMap<>(),
                                        queryParams = new HashMap<>();
                                while (true) {
                                    line = readline(inputStream);
                                    if (line == null) {
                                        socket.close();
                                        return;
                                    }
                                    // System.out.println(line);
                                    if (line.isEmpty()) {
                                        break;
                                    }
                                    int colonPos = line.indexOf(':', 1);
                                    if (colonPos != -1) {
                                        requestHeaders.put(line.substring(0, colonPos).toLowerCase(), line.substring(colonPos + 1).stripLeading());
                                    }
                                }
                                byte[] bodyBuf;
                                if (requestHeaders.containsKey("content-length")) {
                                    int contentLength = Integer.parseInt(requestHeaders.get("content-length"));
                                    bodyBuf = new byte[contentLength];
                                    int received = 0;
                                    while (received < contentLength) {
                                        int newReceived = inputStream.read(bodyBuf, received, contentLength - received);
                                        if (newReceived == -1) {
                                            socket.close();
                                            return;
                                        }
                                        received += newReceived;
                                    }
                                    // System.out.println("Received body content:\n" + new String(bodyBuf));
                                } else {
                                    bodyBuf = new byte[0];
                                }
                                if (!requestHeaders.containsKey("host")) {
                                    throw new IOException("400 Invalid Request: No host header");
                                }
                                if (path.contains("?")) {
                                    String[] pathParts = path.split("\\?", 2);
                                    path = pathParts[0];
                                    parseQueryString(pathParts[1], queryParams);
                                }
                                if ("application/x-www-form-urlencoded".equalsIgnoreCase(requestHeaders.get("content-type"))) {
                                    String bodyStr = new String(bodyBuf, StandardCharsets.UTF_8);
                                    parseQueryString(bodyStr, queryParams);
                                }
                                if ("HEAD".equalsIgnoreCase(method)) {
                                    response.setShouldIgnoreBody(true);
                                }
                                final RequestImpl primitiveRequest = new RequestImpl(method, path, protocol, requestHeaders, queryParams, Collections.emptyMap(),
                                        (InetSocketAddress) socket.getRemoteSocketAddress(), bodyBuf, this, response);
                                for (Filter filter : PRE_FILTERS) {
                                    filter.handle(primitiveRequest, response);
                                    if (response.isHandled()) {
                                        break;
                                    }
                                }
                                if (!response.isHandled()) {
                                    for (var entry : ROUTE_TABLE.getOrDefault(requestHeaders.get("host").split(":")[0].toLowerCase(),
                                                    ROUTE_TABLE.getOrDefault("", Collections.emptyMap()))
                                            .getOrDefault(method.toUpperCase(), Collections.emptyList())) {
                                        Map<String, String> params;
                                        if ((params = tryMatchRoute(entry.getKey(), path)) != null) {
                                            Object returnedValue = entry.getValue().handle(
                                                    new RequestImpl(method, path, protocol, requestHeaders, queryParams, params,
                                                            (InetSocketAddress) socket.getRemoteSocketAddress(), bodyBuf, this, response),
                                                    response);
                                            if (!response.isWritten() && returnedValue != null) {
                                                if (returnedValue instanceof byte[]) {
                                                    response.bodyAsBytes((byte[]) returnedValue);
                                                } else {
                                                    response.body(returnedValue.toString());
                                                }
                                            }
                                            response.setHandled(true);
                                            break;
                                        }
                                    }
                                    for (int j = POST_FILTERS.size() - 1; j >= 0; j--) {
                                        POST_FILTERS.get(j).handle(primitiveRequest, response);
                                    }
                                }
                            } catch (Exception e) {
                                if (!response.isWritten()) {
                                    if (e.getMessage().matches("\\d+: .*")) {
                                        String[] messageParts = e.getMessage().split(": ", 2);
                                        String[] statusParts = messageParts[0].split(" ", 2);
                                        response.status(Integer.parseInt(statusParts[0]), statusParts[1]);
                                        response.body(messageParts[1]);
                                    } else {
                                        e.printStackTrace();
                                        response.status(500, "Internal Server Error");
                                        response.body(e.getMessage());
                                    }
                                }
                            }
                            response.writeDefault();
                            List<String> values = response.getHeaders().get("connection");
                            if (values != null && values.size() > 0 && values.get(0).equalsIgnoreCase("close")) {
                                // System.out.println("Closing socket " + socket);
                                socket.close();
                                return;
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private Socket getSecureSocket(Socket socket) throws Exception {
        SNIInspector sniInspector = new SNIInspector();
        sniInspector.parseConnection(socket);
        ByteArrayInputStream inputStream = sniInspector.getInputStream();
        SNIHostName sniHostName = sniInspector.getHostName();
        String hostName = sniHostName != null ? sniHostName.getAsciiName() : null;
        String password = PASSWORDS.getOrDefault(hostName, "secret");
        String keyStorePath = KEY_STORES.getOrDefault(hostName, "keystore.jks");
        KeyStore keyStore = KeyStore.getInstance("JKS");
        keyStore.load(new FileInputStream(keyStorePath), password.toCharArray());
        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance("SunX509");
        keyManagerFactory.init(keyStore, password.toCharArray());
        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(keyManagerFactory.getKeyManagers(), null, null);
        return sslContext.getSocketFactory().createSocket(socket, inputStream, true);
    }

    private static void parseQueryString(String str, HashMap<String, String> queryParams) {
        try {
            String[] queryParts = str.split("&");
            for (String queryPart : queryParts) {
                String[] queryParamParts = queryPart.split("=", 2);
                String key = URLDecoder.decode(queryParamParts[0], StandardCharsets.UTF_8),
                        value = URLDecoder.decode(queryParamParts[1], StandardCharsets.UTF_8);
                if (queryParams.containsKey(key)) {
                    queryParams.put(key, queryParams.get(key) + "," + value);
                } else {
                    queryParams.put(key, value);
                }
            }
        } catch (ArrayIndexOutOfBoundsException e) {
            System.out.println("Invalid query string: " + str);
        }
    }

    private static Map<String, String> tryMatchRoute(String template, String actual) {
        String[] templateParts = template.split("/"), actualParts = actual.split("/");
        if (templateParts.length != actualParts.length) {
            return null;
        }
        Map<String, String> params = new HashMap<>();
        for (int i = 0; i < templateParts.length; i++) {
            if (templateParts[i].equalsIgnoreCase(actualParts[i])) {
                continue;
            }
            if (templateParts[i].startsWith(":")) {
                params.put(templateParts[i].substring(1), actualParts[i]);
            } else {
                return null;
            }
        }
        return params;
    }
}
