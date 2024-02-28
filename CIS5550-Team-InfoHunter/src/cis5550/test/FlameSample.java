package cis5550.test;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameRDD;

import java.util.Collections;
import java.util.List;

public class FlameSample {
    public static void run(FlameContext ctx, String[] args) throws Exception {
        double prob = Double.parseDouble(args[0]);
        List<String> list = List.of(args[1].split(" "));

        FlameRDD rdd = ctx.parallelize(list).sample(prob);

        List<String> out = rdd.collect();
        Collections.sort(out);

        StringBuilder result = new StringBuilder();
        for (String s : out) {
            result.append(result.length() == 0 ? "" : ",").append(s);
        }

        ctx.output(result.toString());
    }
}
