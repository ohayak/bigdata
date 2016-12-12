
import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class TopKReducer extends Reducer<NullWritable ,City, NullWritable, City>{
	public int k;
	private TreeMap<Double, City> topKCities = new TreeMap<Double, City>();
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		k = conf.getInt("k", 10);
	}
	public void reduce(NullWritable key, Iterable<City> values, Context context) 
			throws IOException, InterruptedException {
		for (City value : values) {
			topKCities.put(value.getPopulation(), new City(value));
			if (topKCities.size() > k)
				topKCities.remove(topKCities.firstKey());
		}
		for (City c : topKCities.descendingMap().values()) {
			context.write(key, c);
		}
	}
}