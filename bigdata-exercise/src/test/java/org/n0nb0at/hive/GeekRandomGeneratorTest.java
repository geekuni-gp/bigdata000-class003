package org.n0nb0at.hive;

import org.junit.Assert;
import org.junit.Test;
import org.n0nb0at.hive.util.GeekRandomGenerator;

public class GeekRandomGeneratorTest {

    @Test
    public void testDefaultGenerator() {
        GeekRandomGenerator generator = new GeekRandomGenerator();
        Integer randomNo = generator.generate();

        Assert.assertTrue("randomNo is not between of [2, 255]", isBetweenOf(randomNo, 2, 255));
    }

    @Test
    public void testGenerator1() {
        GeekRandomGenerator generator = new GeekRandomGenerator(2, 3);
        Integer randomNo = generator.generate();

        Assert.assertTrue("randomNo is not between of [2, 3]", isBetweenOf(randomNo, 2, 3));
    }

    @Test
    public void testGenerator2() {
        GeekRandomGenerator generator = new GeekRandomGenerator(2, 2);
        Integer randomNo = generator.generate();

        Assert.assertTrue("randomNo is not between of [2, 2]", isBetweenOf(randomNo, 2, 2));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGenerator3() {
        GeekRandomGenerator generator = new GeekRandomGenerator(3, 2);
        Integer randomNo = generator.generate();

        Assert.assertTrue("randomNo is not between of [3, 2]", isBetweenOf(randomNo, 2, 2));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGenerator4() {
        GeekRandomGenerator generator = new GeekRandomGenerator(-1, 2);
        Integer randomNo = generator.generate();

        Assert.assertTrue("randomNo is not between of [-1, 2]", isBetweenOf(randomNo, -1, 2));
    }

    private boolean isBetweenOf(Integer target, Integer min, Integer max) {
        return target.compareTo(min) >= 0 && target.compareTo(max) <= 0;
    }
}
