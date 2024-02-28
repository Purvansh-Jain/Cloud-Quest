package cis5550.tools;

import java.util.AbstractMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public class StringUtil {
    public static String generateRandomAlphabeticString() {
        char randomLetter = (char) (ThreadLocalRandom.current().nextInt(26) + 'a');
        String uuid = UUID.randomUUID().toString();
        return randomLetter + uuid;
    }

    public static int getCommonPrefixLength(String a, String b) {
        int i = 0;
        while (i < a.length() && i < b.length() && a.charAt(i) == b.charAt(i)) {
            i++;
        }
        return i;
    }

    public static Map.Entry<String, String> intersectSimpleRanges(Map.Entry<String, String> a, Map.Entry<String, String> b) {
        String aStart = a.getKey(), aEnd = a.getValue();
        String bStart = b.getKey(), bEnd = b.getValue();
        String start =
                aStart == null ? bStart :
                        bStart == null ? aStart :
                                aStart.compareTo(bStart) > 0 ? aStart : bStart;
        String end =
                aEnd == null ? bEnd :
                        bEnd == null ? aEnd :
                                aEnd.compareTo(bEnd) < 0 ? aEnd : bEnd;
        return start == null || end == null || start.compareTo(end) < 0 ?
                new AbstractMap.SimpleImmutableEntry<>(start, end) :
                null;
    }
}
