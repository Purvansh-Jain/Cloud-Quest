package cis5550.test;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class FlameCogroup {
    public static void run(FlameContext ctx, String[] args) throws Exception {
        List<String> argList1 = Arrays.asList(args[0].split(" "));
        List<String> argList2 = Arrays.asList(args[1].split(" "));

        FlamePairRDD rdd = ctx.parallelize(argList1).mapToPair(s -> {
            String[] split = s.split(",");
            return new FlamePair(split[0], split[1]);
        }).cogroup(ctx.parallelize(argList2).mapToPair(s -> {
            String[] split = s.split(",");
            return new FlamePair(split[0], split[1]);
        }));

        List<FlamePair> out = rdd.collect();
        Collections.sort(out);

        StringBuilder result = new StringBuilder();
        for (FlamePair s : out) {
            result.append(result.toString().equals("") ? "" : ",").append(s);
        }

        ctx.output(result.toString());
    }
}
