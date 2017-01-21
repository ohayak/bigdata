import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class CsvReader extends RecordReader<LongWritable, RunnerWritable> {

	private Random rand = new Random();
	private LongWritable key;
	private long offset = 0;
	private RunnerWritable value;
	private long nbPoints;


	@Override
	public LongWritable getCurrentKey() throws IOException,InterruptedException {
		return key;
	}

	@Override
	public RunnerWritable getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return (key.get()-offset)/nbPoints;
	}

	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context)throws IOException, InterruptedException {
		CSVInputSplit split = (CSVInputSplit) genericSplit;
		nbPoints = split.getLength();
		key = new LongWritable(split.getOffset());
		offset = split.getOffset();
		value = new RunnerWritable(rand.nextDouble(), rand.nextDouble());
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		key.set(key.get()+1);
		value = new RunnerWritable(rand.nextDouble(), rand.nextDouble());
		return key.get() < nbPoints ;
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
	}
}
