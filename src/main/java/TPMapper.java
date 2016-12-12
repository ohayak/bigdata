
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TPMapper extends Mapper<LongWritable, Point2DWritable,Text, BooleanWritable>{

	public void map(LongWritable key, Point2DWritable value, Context context) throws IOException, InterruptedException {
		try {
			double  x = value.getX();
			double y = value.getY();
			if(Math.pow(value.getX(),2)+Math.pow(value.getY(),2)<1)
				context.write(new Text("R") ,new BooleanWritable(true));
			else
				context.write(new Text("R"),new BooleanWritable(false));	
		}
		catch (NumberFormatException e) {
			e.printStackTrace();
		}
	}
}