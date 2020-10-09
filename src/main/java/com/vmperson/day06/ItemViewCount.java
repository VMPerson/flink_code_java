package com.vmperson.day06;

import java.sql.Timestamp;

/**
 * @ClassName: ItemViewCount
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/10/9  11:46
 * @Version: 1.0
 */
public class ItemViewCount {

    public String itemId;
    public Long count;
    public Long windowStart;
    public Long windowEnd;

    public ItemViewCount() {
    }

    public ItemViewCount(String itemId, Long count, Long windowStart, Long windowEnd) {
        this.itemId = itemId;
        this.count = count;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
    }

    @Override
    public String toString() {
        return "ItemViewCount{" +
                "itemId='" + itemId + '\'' +
                ", count=" + count +
                ", windowStart=" + new Timestamp(windowStart) +
                ", windowEnd=" + new Timestamp(windowEnd) +
                '}';
    }
}