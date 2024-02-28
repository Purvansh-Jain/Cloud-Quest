package cis5550.kvs;

import cis5550.webserver.Server;

public class Master extends cis5550.generic.Master {
    public static void main(String[] args) {
        Server.port(Integer.parseInt(args[0]));
        registerRoutes();
        Server.get("/", (req, res) -> {
            res.type("text/html");
            return "<!DOCTYPE html>" +
                    "<html>" +
                    "<head>" +
                    "<title>KVS Master</title>" +
                    "</head>" +
                    "<body>" +
                    "<h1>KVS Master</h1>" +
                    workerTable() +
                    "</body>" +
                    "</html>";
        });
    }
}
