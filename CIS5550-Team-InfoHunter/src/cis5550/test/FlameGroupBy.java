package cis5550.test;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;

import java.util.Collections;
import java.util.List;

public class FlameGroupBy {
    public static void run(FlameContext ctx, String[] args) throws Exception {
        List<String> list = List.of(args);

        var rdd = ctx.parallelize(list).groupBy(s -> String.valueOf(s.charAt(0)));

        List<FlamePair> out = rdd.collect();
        Collections.sort(out);

        StringBuilder result = new StringBuilder();
        for (FlamePair s : out) {
            result.append(result.length() == 0 ? "" : ",").append(s._1()).append(":").append(s._2());
        }

        ctx.output(result.toString());
    }
}
