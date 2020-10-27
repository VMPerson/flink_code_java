package com.vmperson.day10;

/**
 * @ClassName: ApacheLogFileds
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/10/14  9:42
 * @Version: 1.0
 */
public class ApacheLogFileds {

    public String ip;
    public String userId;
    public Long eventTime;
    public String method;
    public String url;

    public ApacheLogFileds() {
    }

    public ApacheLogFileds(String ip, String userId, Long eventTime, String method, String url) {
        this.ip = ip;
        this.userId = userId;
        this.eventTime = eventTime;
        this.method = method;
        this.url = url;
    }

    @Override
    public String toString() {
        return "ApacheLogFileds{" +
                "ip='" + ip + '\'' +
                ", userId='" + userId + '\'' +
                ", eventTime=" + eventTime +
                ", method='" + method + '\'' +
                ", url='" + url + '\'' +
                '}';
    }
}