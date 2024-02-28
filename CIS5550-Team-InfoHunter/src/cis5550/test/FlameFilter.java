package cis5550.test;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class FlameFilter {
    public static void run(FlameContext ctx, String[] args) throws Exception {
        List<String> argList = Arrays.asList(args);

        FlameRDD rdd = ctx.parallelize(argList).filter(s -> s.length() > 3);

        List<String> out = rdd.collect();
        Collections.sort(out);

        ctx.output(out.toString());
    }
}
