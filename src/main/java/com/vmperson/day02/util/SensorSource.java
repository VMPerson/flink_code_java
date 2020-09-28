package com.vmperson.day02.util;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Calendar;
import java.util.Random;

/**
 * @ClassName: SensorSource
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/9/27  11:45
 * @Version: 1.0
 */
public class SensorSource extends RichParallelSourceFunction<SensorReading> {

    private Boolean running = true;


    @Override
    public void run(SourceContext ctx) throws Exception {
        Random random = new Random();
        String[] str = new String[10];
        Double[] ctxType = new Double[10];
        for (int i = 0; i < 10; i++) {
            str[i] = "source_" + (i + 1);
            ctxType[i] = 65 + (random.nextGaussian() * 20);
        }

        while (running) {
            long millis = Calendar.getInstance().getTimeInMillis();
            for (int i = 0; i < 10; i++) {
                ctxType[i] +=random.nextGaussian()*0.5;
                ctx.collect(new SensorReading(str[i],millis,ctxType[i]));
            }
        }
    }

    @Override
    public void cancel() {
        this.running=false;
    }
}