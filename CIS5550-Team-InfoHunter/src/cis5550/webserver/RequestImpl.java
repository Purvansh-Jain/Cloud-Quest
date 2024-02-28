package cis5550.webserver;

import java.util.*;
import java.net.*;
import java.nio.charset.*;

import static cis5550.tools.StringUtil.generateRandomAlphabeticString;

// Provided as part of the framework code

class RequestImpl implements Request {
    String method;
    String url;
    String protocol;
    InetSocketAddress remoteAddr;
    Map<String, String> headers;
    Map<String, String> queryParams;
    Map<String, String> params;
    byte[] bodyRaw;
    Server server;
    Response response;
    Session cachedSession = null;

    RequestImpl(String methodArg, String urlArg, String protocolArg,
                Map<String, String> headersArg, Map<String, String> queryParamsArg, Map<String, String> paramsArg,
                InetSocketAddress remoteAddrArg, byte[] bodyRawArg, Server serverArg, Response responseArg) {
        method = methodArg;
        url = urlArg;
        remoteAddr = remoteAddrArg;
        protocol = protocolArg;
        headers = headersArg;
        queryParams = queryParamsArg;
        params = paramsArg;
        bodyRaw = bodyRawArg;
        server = serverArg;
        response = responseArg;
    }

    @Override
    public String requestMethod() {
        return method;
    }

    public void setParams(Map<String, String> paramsArg) {
        params = paramsArg;
    }

    @Override
    public int port() {
        return remoteAddr.getPort();
    }

    @Override
    public String url() {
        return url;
    }

    @Override
    public String protocol() {
        return protocol;
    }

    @Override
    public String contentType() {
        return headers.get("content-type");
    }

    @Override
    public String ip() {
        return remoteAddr.getAddress().getHostAddress();
    }

    @Override
    public String body() {
        return new String(bodyRaw, StandardCharsets.UTF_8);
    }

    @Override
    public byte[] bodyAsBytes() {
        return bodyRaw;
    }

    @Override
    public int contentLength() {
        return bodyRaw.length;
    }

    @Override
    public String headers(String name) {
        return headers.get(name.toLowerCase());
    }

    @Override
    public Set<String> headers() {
        return headers.keySet();
    }

    @Override
    public String queryParams(String param) {
        return queryParams.get(param);
    }

    @Override
    public Set<String> queryParams() {
        return queryParams.keySet();
    }

    @Override
    public String params(String param) {
        return params.get(param);
    }

    @Override
    public Session session() {
        if (cachedSession != null) {
            return cachedSession;
        }
        String sessionId = Util.searchKVString(headers.get("cookie"), "SessionID", ";\\s*", "=");
        long now = System.currentTimeMillis();
        if (sessionId != null) {
            SessionImpl session = (SessionImpl) Server.getSession(sessionId);
            if (session != null
                    && session.getIsValid()
                    && now - session.lastAccessedTime() < session.maxActiveInterval() * 1000L) {
                session.lastAccessedTime(now);
                return cachedSession = session;
            }
        }
        sessionId = generateRandomAlphabeticString();
        Session session = new SessionImpl(sessionId, now);
        Server.addSession(sessionId, session);
        StringBuilder setCookieStringBuilder = new StringBuilder();
        setCookieStringBuilder.append("SessionID=");
        setCookieStringBuilder.append(sessionId);
        setCookieStringBuilder.append("; HttpOnly; SameSite=Lax");
        ResponseImpl responseImpl = (ResponseImpl) response;
        if (responseImpl != null && responseImpl.isConnectionSecure()) {
            setCookieStringBuilder.append("; Secure");
        }
        response.header("set-cookie", setCookieStringBuilder.toString());
        return cachedSession = session;
    }

    @Override
    public Map<String, String> params() {
        return params;
    }

}
