package org.n0nb0at.hive;

import com.google.common.base.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.n0nb0at.hive.codec.GeekCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class GeekTextInputFormatExtendText extends TextInputFormat implements JobConfigurable {
    private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

    @Override
    public RecordReader<LongWritable, Text> getRecordReader(
            InputSplit genericSplit, JobConf job,
            Reporter reporter)
            throws IOException {
        LOGGER.info("");

        reporter.setStatus(genericSplit.toString());
        String delimiter = job.get("textinputformat.record.delimiter");
        byte[] recordDelimiterBytes = null;
        if (null != delimiter) {
            recordDelimiterBytes = delimiter.getBytes(Charsets.UTF_8);
        }
        // 使用自定义 RecordReader
        return new GeekRecordReaderExtendLind(job, (FileSplit) genericSplit,
                recordDelimiterBytes);
    }

    public static class GeekRecordReaderExtendLind extends LineRecordReader {

        private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

        public GeekRecordReaderExtendLind(Configuration job, FileSplit split, byte[] recordDelimiter) throws IOException {
            super(job, split, recordDelimiter);
        }

        @Override
        public boolean next(LongWritable key, Text value) throws IOException {

            replaceCustomStr(value);
            return super.next(key, value);
        }

        private void replaceCustomStr(Text value) {
            try {
                // 按自定义编解码器进行编码
                GeekCodec codec = GeekCodec.createInstance();
                // Text.toString() 会将 bytes 组织成字符串文本
                value.set(codec.decode(value.toString()) + "testappend");
            } catch (Exception e) {
                LOGGER.error("replaceCustomStr error: ", e);
            }
        }
    }
}
