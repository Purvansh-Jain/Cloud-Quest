package cis5550.webserver;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class Util {
    public static void parseKVString(String source, Map<String, String> destination,
                                     String pairSeparator, String keyValueSeparator) {
        if (source == null || destination == null || pairSeparator == null || keyValueSeparator == null) {
            return;
        }
        String[] queryParts = source.split(pairSeparator);
        for (String queryPart : queryParts) {
            String[] queryParamParts = queryPart.split(keyValueSeparator, 2);
            if (queryParamParts.length == 2) {
                destination.put(queryParamParts[0], queryParamParts[1]);
            }
        }
    }

    public static String searchKVString(String source, String key,
                                        String pairSeparator, String keyValueSeparator) {
        if (source == null || key == null || pairSeparator == null || keyValueSeparator == null) {
            return null;
        }
        String[] queryParts = source.split(pairSeparator);
        for (String queryPart : queryParts) {
            String[] queryParamParts = queryPart.split(keyValueSeparator, 2);
            if (queryParamParts.length == 2 && queryParamParts[0].equals(key)) {
                return queryParamParts[1];
            }
        }
        return null;
    }
}
