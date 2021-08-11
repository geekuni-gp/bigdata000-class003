package org.n0nb0at.hive;

import org.junit.Assert;
import org.junit.Test;
import org.n0nb0at.hive.codec.GeekCodec;
import org.n0nb0at.hive.util.GeekRandomGenerator;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class GeekCodecTest {

    @Test
    public void testDecode() {

        // 题目给出的测试用例
        GeekCodec codec = GeekCodec.createInstance();
        String raw = "This notebook can be geeeek used to geek install gek on all geeeek worker nodes, " +
                "run data generation, and create the TPCDS geeeeeeeeek database.";

        String expect = "This notebook can be used to install gek on all worker nodes, " +
                "run data generation, and create the TPCDS database.";

        Assert.assertEquals("codec.decode() fail", codec.decode(raw), expect);
    }

    @Test
    public void testEncode() {

        // 为便于测试，随机数生成器固定生成指定的数值
        GeekCodec codec = GeekCodec.createInstance(new GeekRandomGenerator(3, 3));

        String raw = "This notebook can be used to install gek on all worker nodes, " +
                "run data generation, and create the TPCDS database.";

        String expect = "This notebook can geeek be used to geeek install gek on geeek all worker nodes, geeek " +
                "run data generation, geeek and create the geeek TPCDS database.";

        Assert.assertEquals("codec.encode() fail", codec.encode(raw), expect);
    }

    @Test
    public void testEncode2() throws InterruptedException {

        GeekCodec codec = GeekCodec.createInstance(new GeekRandomGenerator(2, 20));
        for (int i = 0; i < 10 ; i++) {

            String raw = "This notebook can be used to install gek on all worker nodes, " +
                    "run data generation, and create the TPCDS database.";

            System.out.println(codec.encode(raw));
            Thread.sleep(100);
        }
    }

    @Test
    public void testEncode3() {
        GeekCodec codec = GeekCodec.createInstance();
        File input = new File("src/test/resources/hive-homework-format-text.txt");
        File output = new File("src/test/resources/hive-homework-formatted-text.txt");
        try {
            FileInputStream fis = new FileInputStream(input);
            FileOutputStream fos = new FileOutputStream(output);
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fis, StandardCharsets.UTF_8));
            BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fos, StandardCharsets.UTF_8));
            String readLine = null;
            while ((readLine = bufferedReader.readLine()) != null) {
                bufferedWriter.write(codec.encode(readLine));
                System.out.println(codec.encode(readLine));
                bufferedWriter.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
