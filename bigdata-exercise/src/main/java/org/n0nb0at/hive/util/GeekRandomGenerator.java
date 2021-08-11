package org.n0nb0at.hive.util;

import com.google.common.annotations.VisibleForTesting;

import java.util.Random;

public class GeekRandomGenerator {

    private int floor = 2;
    private int ceiling = 255;
    private int bound;
    private int origin;

    public GeekRandomGenerator() {
        calculateBound();
    }

    /**
     * 为便于测试，使用自定义的上下限入参控制随机数范围
     *
     * @param floor   下限
     * @param ceiling 上限
     */
    @VisibleForTesting
    public GeekRandomGenerator(int floor, int ceiling) {
        if (ceiling < 0 || floor < 0) {
            throw new IllegalArgumentException("ceiling and floor must be greater than zero");
        }
        this.floor = floor;
        this.ceiling = ceiling;
        calculateBound();
    }

    /**
     * 通过认为可读的入参计算上下限
     */
    private void calculateBound() {
        if (ceiling < floor) {
            throw new IllegalArgumentException("floor is bigger than ceiling");
        }
        this.bound = this.ceiling - this.floor + 1;
        this.origin = floor;
    }

    public Integer generate() {
        return new Random().nextInt(bound) + origin;
    }
}
