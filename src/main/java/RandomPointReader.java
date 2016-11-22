import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class RandomPointReader extends RecordReader {

	private Random rand = new Random();
	private LongWritable key;
	private Point2DWritable value;
	private long maxSplits;


	@Override
	public LongWritable getCurrentKey() throws IOException,InterruptedException {
		return key;
	}

	@Override
	public Point2DWritable getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return key.get();
	}

	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context)throws IOException, InterruptedException {
		FakeInputSplit split = (FakeInputSplit) genericSplit;
		maxSplits = split.getLength();
		key = new LongWritable(split.getOffset());
		value = new Point2DWritable(rand.nextDouble(), rand.nextDouble());
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		key.set(key.get()+1);
		value = new Point2DWritable(rand.nextDouble(), rand.nextDouble());
		if (key.get() < maxSplits )
			return true;
		else 
			return false;
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
	}
}
