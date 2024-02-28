package cis5550.test;

import cis5550.flame.FlameSubmit;

public class IntersectionTest {
    public static void main(String[] args) throws Exception {
        System.out.println(
                FlameSubmit.submit("localhost:9000", "tests/flame-intersection.jar", "cis5550.test.FlameIntersection", args)
        );
    }
}
