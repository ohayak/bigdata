import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class AvgWritable implements Writable {
    private int count;
    private int max;
    private int sum;
    private int min;
    private double avg;

    public AvgWritable() {
        super();
    }

    public AvgWritable(int sum, int count, int max, int min) {
        super();
        this.sum = sum;
        this.count = count;
        this.max = max;
        this.min = min;
        this.avg = 0;
    }

    public AvgWritable(int sum, int count, int max, int min, double avg) {
        super();
        this.sum = sum;
        this.count = count;
        this.max = max;
        this.min = min;
        this.avg = avg;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        sum = in.readInt();
        count = in.readInt();
        max = in.readInt();
        min = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(sum);
        out.writeInt(count);
        out.writeInt(max);
        out.writeInt(min);
        out.writeDouble(avg);
    }

    public int getSum() {
        return sum;
    }

    public void setSum(int sum) {
        this.sum = sum;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public int getMax() {
        return max;
    }

    public void setMax(int max) {
        this.max = max;
    }

    public int getMin() {
        return min;
    }

    public void setMin(int min) {
        this.min = min;
    }

    public double getAvg() {
        return avg;
    }

    public void setAvg(double avg) {
        this.avg = avg;
    }

    @Override
    public String toString() {

        return "\t" + count + "\t" + avg + "\t" + max + "\t" + min;

    }


}
