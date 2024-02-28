package cis5550.test;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameRDD;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class FlameMapPartitions {
    public static void run(FlameContext ctx, String[] args) throws Exception {
        List<String> argList = Arrays.asList(args);

        FlameRDD rdd = ctx.parallelize(argList).mapPartitions(iter -> {
            ArrayList<String> output = new ArrayList<>();
            while (iter.hasNext()) {
                output.add(iter.next() + "!");
            }
            return output.iterator();
        });

        List<String> out = rdd.collect();
        Collections.sort(out);

        ctx.output(out.toString());
    }
}
