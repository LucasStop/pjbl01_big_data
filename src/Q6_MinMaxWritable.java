import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Q6_MinMaxWritable implements Writable {

    private double min;
    private double max;

    public Q6_MinMaxWritable() {
        this.min = Double.MAX_VALUE;
        this.max = -Double.MAX_VALUE;
    }

    public Q6_MinMaxWritable(double min, double max) {
        this.min = min;
        this.max = max;
    }

    public double getMin() {
        return min;
    }

    public void setMin(double min) {
        this.min = min;
    }

    public double getMax() {
        return max;
    }

    public void setMax(double max) {
        this.max = max;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(min);
        out.writeDouble(max);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        min = in.readDouble();
        max = in.readDouble();
    }

    @Override
    public String toString() {
        return "min=" + min + ", max=" + max;
    }
}
