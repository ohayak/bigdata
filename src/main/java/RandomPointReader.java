import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class RandomPointReader extends RecordReader {

	private Random rand = new Random();
    private IntWritable key;
    private Point2DWritable value;
    private long maxSplits;
    private long offset;
 
@Override
    public void close() throws IOException {
    }
 
@Override
    public IntWritable getCurrentKey() throws IOException,InterruptedException {
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
        offset = split.getOffset();
        key = new IntWritable((int)offset);
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
}
