package cis5550.webserver;

public class SessionUpdateFilter implements Filter {
    @Override
    public void handle(Request request, Response response) throws Exception {
        RequestImpl requestImpl = (RequestImpl) request;
        if (requestImpl == null) {
            return;
        }
        if (requestImpl.cachedSession == null) {
            String sessionId = Util.searchKVString(requestImpl.headers.get("cookie"), "SessionID", ";\\s*", "=");
            if (sessionId != null) {
                SessionImpl session = (SessionImpl) Server.getSession(sessionId);
                if (session != null) {
                    session.lastAccessedTime(System.currentTimeMillis());
                }
            }
        }
    }
}
