import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;

public class FakeInputSplit extends InputSplit{

	private long length;
	private long offset;

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

}
