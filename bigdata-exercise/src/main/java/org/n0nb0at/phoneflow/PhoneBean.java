package org.n0nb0at.phoneflow;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Setter
@Getter
public class PhoneBean implements WritableComparable<PhoneBean> {

    private String phoneNo;

    @Override
    public int compareTo(PhoneBean o) {
        return this.getPhoneNo().compareTo(o.getPhoneNo());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.getPhoneNo());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.phoneNo = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return phoneNo + "\t";
    }
}
