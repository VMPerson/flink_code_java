package com.vmperson.day10;


import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.*;

/**
 * @ClassName: SimulatedEventSource
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/10/14  10:50
 * @Version: 1.0
 */
public class SimulatedEventSource extends RichParallelSourceFunction<MarketingUserBehavior> {

    boolean running = true;

    List<String> storeList = new ArrayList<String>() {{
        add("AppStore");
        add("XiaomiStore");
    }};

    List<String> behaviorTypes = Lists.newArrayList("BROWSE", "CLICK");

    Random random = new Random();

    @Override
    public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {

        while (running) {
            String userId = UUID.randomUUID().toString();
            String behaviorTypes = this.behaviorTypes.get(random.nextInt(this.behaviorTypes.size()));
            String channel = storeList.get(random.nextInt(storeList.size()));
            long millis = Calendar.getInstance().getTimeInMillis();
            ctx.collect(new MarketingUserBehavior(userId, behaviorTypes, channel, millis));
            Thread.sleep(10);
        }

    }

    @Override
    public void cancel() {
        running = false;
    }
}