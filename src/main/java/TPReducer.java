
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TPReducer extends Reducer<LongWritable, Text, LongWritable, Text>{

	public void reduce(LongWritable key, Text  value, Context context) throws IOException, InterruptedException {
		try {
			context.write(key,value);
		}
		catch (NumberFormatException e) {
			e.printStackTrace();
		}
	}
}