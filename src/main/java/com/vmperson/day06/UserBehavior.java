package com.vmperson.day06;

import java.sql.Timestamp;

/**
 * @ClassName: UserBehavior
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/10/9  11:46
 * @Version: 1.0
 */
public class UserBehavior {
    public String userId;
    public String itemId;
    public String categoryId;
    public String behavior;
    public Long timestamp;

    public UserBehavior() {
    }

    public UserBehavior(String userId, String itemId, String categoryId, String behavior, Long timestamp) {
        this.userId = userId;
        this.itemId = itemId;
        this.categoryId = categoryId;
        this.behavior = behavior;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "UserBehavior{" +
                "userId='" + userId + '\'' +
                ", itemId='" + itemId + '\'' +
                ", categoryId='" + categoryId + '\'' +
                ", behavior='" + behavior + '\'' +
                ", timestamp=" + new Timestamp(timestamp) +
                '}';
    }
}