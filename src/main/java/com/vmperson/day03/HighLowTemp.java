package com.vmperson.day03;

import java.sql.Timestamp;

/**
 * @ClassName: HighLowTemp
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/9/28  14:25
 * @Version: 1.0
 */
public class HighLowTemp {

    public String id;
    public Double low, high;
    public Long startTime, endTime;

    public HighLowTemp() {
    }

    public HighLowTemp(String id, Double low, Double high, Long startTime, Long endTime) {
        this.id = id;
        this.low = low;
        this.high = high;
        this.startTime = startTime;
        this.endTime = endTime;
    }

    @Override
    public String toString() {
        return "HighLowTemp{" +
                "id='" + id + '\'' +
                ", low=" + low +
                ", high=" + high +
                ", startTime=" + new Timestamp(startTime) +
                ", endTime=" + new Timestamp(endTime) +
                '}';
    }
}