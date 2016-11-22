
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class TPReducer extends Reducer<LongWritable, Point2DWritable, LongWritable, Point2DWritable>{

	public void reduce(LongWritable key, Point2DWritable value, Context context) throws IOException, InterruptedException {
		try {
			context.write(key,value);
		}
		catch (NumberFormatException e) {
			e.printStackTrace();
		}
	}
}