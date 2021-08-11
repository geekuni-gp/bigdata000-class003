package org.n0nb0at.hive.codec;

import org.apache.commons.lang3.StringUtils;
import org.n0nb0at.hive.util.GeekRandomGenerator;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class GeekCodec implements GeekEncoder, GeekDecoder {

    private final GeekRandomGenerator randomGenerator;

    public static GeekCodec createInstance() {
        return new GeekCodec();
    }

    public static GeekCodec createInstance(GeekRandomGenerator randomGenerator) {
        try {
            Constructor<GeekCodec> ctor = GeekCodec.class.getConstructor(GeekRandomGenerator.class);
            return ctor.newInstance(randomGenerator);
        } catch (NoSuchMethodException nme) {
            return new GeekCodec();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (InvocationTargetException ite) {
            throw new RuntimeException(ite.getCause());
        }
    }

    public GeekCodec() {
        this.randomGenerator = new GeekRandomGenerator();
    }

    public GeekCodec(GeekRandomGenerator randomGenerator) {
        this.randomGenerator = randomGenerator;
    }


    @Override
    public String decode(String raw) {
        // 将「符合规则的单词」及其「前面可能包含的空格」，替换为空字符
        return raw.replaceAll("\\s*ge{2,}k", StringUtils.EMPTY);
    }

    @Override
    public String encode(String raw) {
        StringBuilder sb = new StringBuilder();
        String[] words = raw.split(" ");

        Integer randomNo = randomGenerator.generate();
        int size = words.length;
        // offset 用于记录所有单词的拼接偏移量
        int offset = 0;
        // insertOffset 用于记录下一次插入自定义单词的位置
        int insertOffset = randomNo;

        // 插入便宜要小于句子总单词数
        int insertedCount = 0;
        while (insertOffset < size + insertedCount) {
            // 将下一次插入自定义单词之前的位置先拼接
            for (; offset + insertedCount < insertOffset; offset++) {
                sb.append(words[offset]);
                // 最后一个单词之后不追加空格
                if (offset != size - 1) {
                    sb.append(" ");
                }
            }
            // 将自定义单词拼接上
            sb.append(generateCustomWord(randomNo)).append(" ");
            // 原句单词数量 + 1，用于下一次是否可插入的判断
            insertedCount++;
            insertOffset++;
            // 继续机选下一次插入位置
            randomNo = randomGenerator.generate();
            insertOffset += randomNo;
        }

        // 剩余未拼接完的内容继续拼接
        for (; offset + insertedCount < size + insertedCount; offset++) {
            sb.append(words[offset]);
            // 最后一个单词之后不追加空格
            if (offset != size - 1) {
                sb.append(" ");
            }
        }

        return sb.toString();
    }

    /**
     * 根据随机数生成自定义单词
     *
     * @param randomNo
     * @return
     */
    private String generateCustomWord(int randomNo) {
        StringBuilder randomWord = new StringBuilder();
        randomWord.append("g");
        for (int i = 0; i < randomNo; i++) {
            randomWord.append("e");
        }
        randomWord.append("k");
        return randomWord.toString();
    }
}
