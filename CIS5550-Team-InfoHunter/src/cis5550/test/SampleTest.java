package cis5550.test;

import cis5550.flame.FlameSubmit;

public class SampleTest {
    public static void main(String[] args) throws Exception {
        System.out.println(
                FlameSubmit.submit("localhost:9000", "tests/flame-sample.jar", "cis5550.test.FlameSample", args)
        );
    }
}
