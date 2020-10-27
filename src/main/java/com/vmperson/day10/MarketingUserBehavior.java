package com.vmperson.day10;

/**
 * @ClassName: MarketingUserBehavior
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/10/14  10:48
 * @Version: 1.0
 */
public class MarketingUserBehavior {

    public String userId;
    public String behavior;
    public String channel;
    public Long ts;


    public MarketingUserBehavior() {
    }


    public MarketingUserBehavior(String userId, String behavior, String channel, Long ts) {
        this.userId = userId;
        this.behavior = behavior;
        this.channel = channel;
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "MarketingUserBehavior{" +
                "userId='" + userId + '\'' +
                ", behavior='" + behavior + '\'' +
                ", channel='" + channel + '\'' +
                ", ts=" + ts +
                '}';
    }
}