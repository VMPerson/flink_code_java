package com.vmperson.day02;

/**
 * @ClassName: Alert
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/9/27  16:13
 * @Version: 1.0
 */
public class Alert {
    public String message;
    public long timestamp;

    public Alert() {
    }


    public Alert(String message, long timestamp) {
        this.message = message;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Alert{" +
                "message='" + message + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}