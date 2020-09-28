package com.vmperson.day02.util;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;

/**
 * @ClassName: SmokeLevelSource
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/9/27  16:09
 * @Version: 1.0
 */
public class SmokeLevelSource extends RichParallelSourceFunction<SmokeLevel> {

    private boolean running = true;

    @Override
    public void run(SourceContext<SmokeLevel> ctx) throws Exception {
        Random rand = new Random();

        while (running) {
            if (rand.nextGaussian() > 0.8) {
                ctx.collect(SmokeLevel.HIGH);
            } else {
                ctx.collect(SmokeLevel.LOW);
            }
            Thread.sleep(100);
        }
    }

    @Override
    public void cancel() {
        this.running = false;
    }
}