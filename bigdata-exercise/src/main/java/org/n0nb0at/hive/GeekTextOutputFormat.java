package org.n0nb0at.hive;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.util.Progressable;
import org.n0nb0at.hive.codec.GeekCodec;
import org.n0nb0at.hive.util.GeekRandomGenerator;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class GeekTextOutputFormat<K extends WritableComparable, V extends Writable>
        extends HiveIgnoreKeyTextOutputFormat<K, V> {

    public GeekTextOutputFormat() {
    }

    public FileSinkOperator.RecordWriter getHiveRecordWriter(JobConf jc,
                                                             Path finalOutPath,
                                                             Class<? extends Writable> valueClass,
                                                             boolean isCompressed,
                                                             Properties tableProperties,
                                                             Progressable progress) throws IOException {



        return new GeekRecordWriter(super.getHiveRecordWriter(jc, finalOutPath,
                BytesWritable.class, isCompressed, tableProperties, progress));
    }

    public static class GeekRecordWriter implements FileSinkOperator.RecordWriter, JobConfigurable {

        FileSinkOperator.RecordWriter writer;
        BytesWritable bytesWritable;
        private byte[] signature;
        GeekCodec geekCodec = GeekCodec.createInstance(new GeekRandomGenerator(2, 20));

        public GeekRecordWriter(FileSinkOperator.RecordWriter writer) {
            this.writer = writer;
            this.bytesWritable = new BytesWritable();
        }


        public void write(Writable w) throws IOException {
            String formatStr;
            if (w instanceof Text) {
                // 更新为编码后的文本
                formatStr = this.geekCodec.encode(((Text) w).toString());
                ((Text) w).set(formatStr);
            } else {
                assert w instanceof BytesWritable;
                // 更新为编码后的文本
                formatStr = this.geekCodec.encode(new String(((BytesWritable) w).getBytes()));
            }

            byte[] output = formatStr.getBytes();

            this.bytesWritable.set(output, 0, output.length);
            this.writer.write(this.bytesWritable);
        }

        public void close(boolean abort) throws IOException {
            this.writer.close(abort);
        }

        @Override
        public void configure(JobConf job) {
            String signatureString = job.get("base64.text.output.format.signature");
            if (signatureString != null) {
                this.signature = signatureString.getBytes(StandardCharsets.UTF_8);
            } else {
                this.signature = new byte[0];
            }
        }
    }
}
