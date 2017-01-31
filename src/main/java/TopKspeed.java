import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

public class TopKspeed extends Configured implements Tool{

	private static class TopKMapper extends Mapper<Text,RunnerWritable,DoubleWritable, Text > {

		private TreeMap<Double, String> topk = new TreeMap<Double, String>();
		private int k = 10;
		private int distance;
		private String gender;
		private String category;

		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			this.k = context.getConfiguration().getInt("K", 10);
			distance =  context.getConfiguration().getInt("D", 10);
			gender =  context.getConfiguration().getStrings("G", new String[]{"MALE|FEMALE"})[0];
			category =  context.getConfiguration().getStrings("C", new String[]{"ALL"})[0];
		}

		@Override
		protected void map(Text key, RunnerWritable value, Context context)
				throws IOException, InterruptedException {
			int d = value.getDistance();
			String gend = value.getGender().toString();
			String cat = value.getCategory().toString();
			boolean bool;
			if (category.equals("ALL"))
				bool = true;
			else 
				bool = cat.contains(category);
			long time = value.getTimeInSec();
			if (d == distance  && gend.matches(gender) && bool && time > 0
					&& (value.getFirstname()!=null && !value.getFirstname().equals( "NDF") && !value.getFirstname().equals( "null")
					|| (value.getLastname()!=null && !value.getLastname().equals( "NDF") && !value.getLastname().equals( "null")))) {
					double speed = (distance*1000.0) / (time*1.0) ;
					topk.put(new Double(speed), value.toString());
			}
			while (topk.size() > k)
				topk.remove(topk.firstKey());
		}

		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			for(Map.Entry<Double, String> pair : topk.entrySet()) {
				context.write(new DoubleWritable(pair.getKey()),new Text(pair.getValue()));
			}
		}
	}

	private static class TopKReducer extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {
		private int k = 10;
		private TreeMap<Double, Iterable<Text>> topk = new TreeMap<Double, Iterable<Text>>();

		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			this.k = context.getConfiguration().getInt("K", 10);
		}

		@Override
		protected void reduce(DoubleWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
				topk.put(key.get(), values);
				while (topk.size() > k)
					topk.remove(topk.firstKey());
		}

		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			for(Map.Entry<Double, Iterable<Text>> pair : topk.entrySet()) {
				String runners = "";
				for (Text txt : pair.getValue()) {
					runners += txt.toString()+"\n\t\t";
				}
				context.write(new DoubleWritable(pair.getKey()),new Text(runners));
			}
		}


	}

	private Configuration conf;

	public Configuration getConf() {
		return conf;
	}

	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	public int run(String[] args) throws Exception {

		Job job = Job.getInstance(conf, "TopKspeed");
		job.setNumReduceTasks(1);
		job.setJarByClass(Main.class);
		FileSystem fs = FileSystem.get(conf);


		job.setMapperClass(TopKMapper.class);
		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(TopKReducer.class);
		job.setOutputKeyClass(DoubleWritable.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(CSVLineInputFormat.class);
		CSVLineInputFormat.setInputPaths(job, args[0]);
		job.setOutputFormatClass(TextOutputFormat.class);
		fs.delete(new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
