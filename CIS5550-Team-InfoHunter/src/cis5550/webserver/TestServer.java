package cis5550.webserver;

import static cis5550.webserver.Server.*;
import static cis5550.webserver.Server.get;

public class TestServer {
    public static void main(String[] args) throws Exception {
        securePort(443);
        get("/", (req, res) -> {
            return "Hello World - this is Xinyue Liu";
        });
    }
}
