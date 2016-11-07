import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TP3Reducer2 extends Reducer<LongWritable, AvgWritable, LongWritable, AvgWritable>{

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
		context.write(key, new AvgWritable(sum,count,max,min,avg));
	}
}
