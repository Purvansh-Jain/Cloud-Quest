package cis5550.test;

import cis5550.flame.FlameSubmit;

public class WordCountTest {
    public static void main(String[] args) throws Exception {
        System.out.println(
                FlameSubmit.submit("localhost:9000", "tests/flame-wordcount.jar", "cis5550.test.FlameWordCount", args)
        );
    }
}
