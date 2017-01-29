
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TPMapper extends Mapper<	Text, RunnerWritable, Text, Text>{

	public void map(Text key, RunnerWritable value, Context context) throws IOException, InterruptedException {
		try {
				context.write(key,new Text(value.toString()));	
		}
		catch (NumberFormatException e) {
			e.printStackTrace();
		}
	}
}