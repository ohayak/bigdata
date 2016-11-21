import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Point2DWritable implements WritableComparable {
	private double x;
	private double y;
	
	public Point2DWritable(double x, double y) {
		this.x = x;
		this.y = y;
	}
	
	public double getX() {
		return x;
	}

	public void setX(double x) {
		this.x = x;
	}

	public double getY() {
		return y;
	}

	public void setY(double y) {
		this.y = y;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(x);
		out.writeDouble(y);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		x = in.readDouble();
		y = in.readDouble();
	}


	@Override
	public int compareTo(Object o) {
		Point2DWritable p = (Point2DWritable) o;
		if (x == p.getX() && y == p.getY())
			return 1;
		else
			return 0;
	}
	
	 public String toString() {
		    return Double.toString(x) + ", "+ Double.toString(y) ;
	 }
}
