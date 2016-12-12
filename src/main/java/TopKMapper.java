
import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopKMapper extends Mapper<Object, Text, NullWritable, City> {
	private TreeMap<Double, City> topKCities = new TreeMap<Double, City>();
	private int k;
	@Override
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		k = conf.getInt("k", 10);
	}
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String tokens[] = value.toString().split(",");
		try {
			Double pop = Double.parseDouble(tokens[4]);
			String name = tokens[0]+tokens[1];
			City city = new City(name, pop);
			topKCities.put(pop, city);
			if(topKCities.size()>k) {
				topKCities.remove(topKCities.firstKey());
			}
		}
		catch (NumberFormatException e) {
			e.printStackTrace();;
		}
	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
		for (City c : topKCities.values()) {
			context.write(NullWritable.get(), c);
		}
	}
}