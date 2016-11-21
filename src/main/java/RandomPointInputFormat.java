import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class RandomPointInputFormat extends InputFormat<IntWritable, Point2DWritable> {

	@Override
	public RecordReader createRecordReader(InputSplit arg0, TaskAttemptContext arg1)
			throws IOException, InterruptedException {
		return new RandomPointReader();
	}

	@Override
	public List getSplits(JobContext arg0) throws IOException, InterruptedException {
		List<FakeInputSplit> list = new ArrayList<FakeInputSplit>();
		for (int i = 0 ; i < 20; i++) {
			list.add(new FakeInputSplit());
		}
		return list;
	}

}
