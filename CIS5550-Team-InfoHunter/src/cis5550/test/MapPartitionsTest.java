package cis5550.test;

import cis5550.flame.FlameSubmit;

public class MapPartitionsTest {
    public static void main(String[] args) throws Exception {
        System.out.println(
                FlameSubmit.submit("localhost:9000", "tests/flame-mapPartitions.jar", "cis5550.test.FlameMapPartitions", args)
        );
    }
}
