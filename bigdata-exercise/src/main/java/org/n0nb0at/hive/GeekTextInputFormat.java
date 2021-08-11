package org.n0nb0at.hive;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.n0nb0at.hive.codec.GeekCodec;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class GeekTextInputFormat implements InputFormat<LongWritable, BytesWritable>, JobConfigurable {
    TextInputFormat format = new TextInputFormat();
    JobConf job;

    public GeekTextInputFormat() {
    }

    public void configure(JobConf job) {
        this.job = job;
        this.format.configure(job);
    }

    public RecordReader<LongWritable, BytesWritable> getRecordReader(InputSplit genericSplit, JobConf job, Reporter reporter) throws IOException {
        reporter.setStatus(genericSplit.toString());
        GeekRecordReader reader = new GeekRecordReader(new LineRecordReader(job, (FileSplit) genericSplit));
        reader.configure(job);
        return reader;
    }

    public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
        return this.format.getSplits(job, numSplits);
    }

    public static class GeekRecordReader implements RecordReader<LongWritable, BytesWritable>, JobConfigurable {
        LineRecordReader reader;
        Text text;
        private byte[] signature;
        GeekCodec geekCodec = GeekCodec.createInstance();

        public GeekRecordReader(LineRecordReader reader) {
            this.reader = reader;
            this.text = reader.createValue();
        }

        public void close() throws IOException {
            this.reader.close();
        }

        public LongWritable createKey() {
            return this.reader.createKey();
        }

        public BytesWritable createValue() {
            return new BytesWritable();
        }

        public long getPos() throws IOException {
            return this.reader.getPos();
        }

        public float getProgress() throws IOException {
            return this.reader.getProgress();
        }

        public boolean next(LongWritable key, BytesWritable value) throws IOException {
            byte[] binaryData;
            int i;
            do {
                if (!this.reader.next(key, this.text)) {
                    return false;
                }

                // 使用自定义解码
                String formatStr = this.geekCodec.decode(this.text.toString());

                this.text.set(formatStr);

                byte[] textBytes = this.text.getBytes();
                int length = this.text.getLength();
                if (length != textBytes.length) {
                    textBytes = Arrays.copyOf(textBytes, length);
                }

                binaryData = textBytes;

                for (i = 0; i < binaryData.length && i < this.signature.length && binaryData[i] == this.signature[i]; ++i) {
                }
            } while (i != this.signature.length);

            value.set(binaryData, this.signature.length, binaryData.length - this.signature.length);
            return true;
        }

        public void configure(JobConf job) {
            String signatureString = job.get("base64.text.input.format.signature");
            if (signatureString != null) {
                this.signature = signatureString.getBytes(StandardCharsets.UTF_8);
            } else {
                this.signature = new byte[0];
            }

        }
    }
}
