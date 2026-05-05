import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Q8_MinMaxAmountWritable implements Writable {

    private double min;
    private double max;

    public Q8_MinMaxAmountWritable() {
        this.min = Double.MAX_VALUE;
        this.max = -Double.MAX_VALUE;
    }

    public Q8_MinMaxAmountWritable(double min, double max) {
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
        return String.format("min=%.2f, max=%.2f", min, max);
    }
}
