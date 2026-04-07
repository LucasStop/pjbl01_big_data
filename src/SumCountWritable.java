import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class SumCountWritable implements Writable {

    private double sum;
    private int count;

    public SumCountWritable() {
        this.sum = 0.0;
        this.count = 0;
    }

    public SumCountWritable(double sum, int count) {
        this.sum = sum;
        this.count = count;
    }

    public double getSum() {
        return sum;
    }

    public void setSum(double sum) {
        this.sum = sum;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(sum);
        out.writeInt(count);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        sum = in.readDouble();
        count = in.readInt();
    }

    @Override
    public String toString() {
        return "sum=" + sum + ", count=" + count;
    }
}
