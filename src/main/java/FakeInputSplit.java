import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

public class FakeInputSplit extends InputSplit implements Writable{

	private long length;
	private long offset;
	public FakeInputSplit() {}
	public FakeInputSplit(long len, long offset) {
		this.length = len;
		this.offset = offset;
	}
	@Override
	public long getLength() throws IOException, InterruptedException {
		return length;
	}

	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		return new String[0];
	}
	
	public long getOffset() {
		return offset;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(length);
		out.writeLong(offset);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		length = in.readLong();
		offset =  in.readLong();
	}

}
