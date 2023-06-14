package unipi.cloudcomputing.randompick;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Sample implements Writable {
    public long randomId;
    public String sample;


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(randomId);
        out.writeUTF(sample);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        randomId = in.readLong();
        sample = in.readUTF();
    }
}
