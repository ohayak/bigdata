
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TPReducer extends Reducer<Text ,BooleanWritable, Text, DoubleWritable>{

	public void reduce(Text key, Iterable<BooleanWritable> values, Context context) throws IOException, InterruptedException {
		try {
			int num_point_valid=0;
			int num_point_total=0;
			for (BooleanWritable itr: values) {
			if(itr.get()==true)
				num_point_valid++;
			num_point_total++;
		  }
			context.write(new Text("Result of the PI calculation _Monte carlo_ : "), new DoubleWritable(new Double(4.*(double)num_point_valid/(double)num_point_total)));
		}
		catch (NumberFormatException e) {
			e.printStackTrace();
		}
	}
}