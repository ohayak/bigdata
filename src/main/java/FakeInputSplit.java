import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;

public class FakeInputSplit extends InputSplit{

	@Override
	public long getLength() throws IOException, InterruptedException {
		return 10;
	}

	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		return new String[0];
	}

	public long getOffset() {
		return 10;
	}

}
