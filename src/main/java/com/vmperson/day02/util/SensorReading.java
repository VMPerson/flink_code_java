package com.vmperson.day02.util;

/**
 * @ClassName: SensorReading
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/9/27  11:44
 * @Version: 1.0
 */
public class SensorReading {

    public String sensorId;
    public long LongTime;
    public Double curFTemp;

    public SensorReading() {
    }

    public SensorReading(String sensorId, long longTime, Double curFTemp) {
        this.sensorId = sensorId;
        LongTime = longTime;
        this.curFTemp = curFTemp;
    }


    @Override
    public String toString() {
        return "SensorReading{" +
                "sensorId='" + sensorId + '\'' +
                ", LongTime=" + LongTime +
                ", curFTemp=" + curFTemp +
                '}';
    }
}