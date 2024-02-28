package cis5550.test;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameRDD;

import java.util.Collections;
import java.util.List;

public class FlameIntersection {
    public static void run(FlameContext ctx, String[] args) throws Exception {
        List<String> list1 = List.of(args[0].split(" "));
        List<String> list2 = List.of(args[1].split(" "));

        FlameRDD rdd1 = ctx.parallelize(list1);
        FlameRDD rdd2 = ctx.parallelize(list2);

        FlameRDD rdd = rdd1.intersection(rdd2);

        List<String> out = rdd.collect();
        Collections.sort(out);

        StringBuilder result = new StringBuilder();
        for (String s : out) {
            result.append(result.length() == 0 ? "" : ",").append(s);
        }

        ctx.output(result.toString());
    }
}
