import java.io.IOException;
import java.io.InterruptedIOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class TP3Reducer2 extends Reducer<LongWritable, AvgWritable, LongWritable, AvgWritable>{
	private int base;

	protected void setup(Mapper.Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		base = Integer.parseInt(conf.get("base"));
	}
	public void reduce(LongWritable key, Iterable<AvgWritable> values, Context context) throws IOException, InterruptedException {
		int i = 0;
		int sum = 0;
		int count = 0;
		int max = -1;
		int min = Integer.MAX_VALUE;
		double avg=0;
		for (AvgWritable itr: values) {
			sum += itr.getSum();
			count+=itr.getCount();
			if(itr.getMax()>max)
				max = itr.getMax();
			if(itr.getMin()<min)
				min = itr.getMin(); 
		}
		avg = sum/count;
		LongWritable keyOut =new LongWritable((long) Math.pow(base, key.get()));
		context.write(keyOut, new AvgWritable(sum,count,max,min,avg));
	}
}
