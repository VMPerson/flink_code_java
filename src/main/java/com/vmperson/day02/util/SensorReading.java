package com.vmperson.day02.util;

/**
 * @ClassName: SensorReading
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/9/27  11:44
 * @Version: 1.0
 */
public class SensorReading {

    private String sensorId;
    private long LongTime;
    private Double curFTemp;

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

    public String getSensorId() {
        return sensorId;
    }

    public void setSensorId(String sensorId) {
        this.sensorId = sensorId;
    }

    public long getLongTime() {
        return LongTime;
    }

    public void setLongTime(long longTime) {
        LongTime = longTime;
    }

    public Double getCurFTemp() {
        return curFTemp;
    }

    public void setCurFTemp(Double curFTemp) {
        this.curFTemp = curFTemp;
    }
}