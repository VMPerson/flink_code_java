package com.vmperson.day10;

/**
 * @ClassName: ApacheLogCount
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/10/14  10:08
 * @Version: 1.0
 */
public class ApacheLogCount {

    public String url;
    public Long winEnd;
    public Long count;

    public ApacheLogCount() {
    }


    public ApacheLogCount(String url, Long winEnd, Long count) {
        this.url = url;
        this.winEnd = winEnd;
        this.count = count;
    }

    @Override
    public String toString() {
        return "ApacheLogCount{" +
                "url='" + url + '\'' +
                ", winEnd=" + winEnd +
                ", count=" + count +
                '}';
    }
}