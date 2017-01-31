import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

public class TopKclub extends Configured implements Tool{

	private static class TopKMapper extends Mapper<Text,RunnerWritable,Text, BooleanWritable > {

		private TreeMap<Double, String> topk = new TreeMap<Double, String>();
		private int k = 10;
		private int distance;
		private String gender;
		private String category;

		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			this.k = context.getConfiguration().getInt("K", 10);
			gender =  context.getConfiguration().getStrings("G", new String[]{"MALE|FEMALE"})[0];
			category =  context.getConfiguration().getStrings("C", new String[]{"ALL"})[0];
		}

		@Override
		protected void map(Text key, RunnerWritable value, Context context)
				throws IOException, InterruptedException {
			String gend = value.getGender().toString();
			String cat = value.getCategory().toString();
			boolean bool;
			if (category.equals("ALL"))
				bool = true;
			else 
				bool = cat.contains(category);
			if (gend.matches(gender) && bool 
					&& !value.getClubName().equals("NDF")) {
					context.write(new Text(value.getClubName()), new BooleanWritable(true));
			}
		}
	}

	private static class TopKReducer extends Reducer<Text, BooleanWritable, LongWritable, Text> {
		private int k = 10;
		private TreeMap<Long, String> topk = new TreeMap<Long, String>();

		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			this.k = context.getConfiguration().getInt("K", 10);
		}

		@Override
		protected void reduce(Text key, Iterable<BooleanWritable> values, Context context)
				throws IOException, InterruptedException {
			long size = 0;
			for (BooleanWritable b : values) {
				size++;
			}
			topk.put(size, key.toString());
			while (topk.size() > k)
				topk.remove(topk.firstKey());
		}

		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			for(Map.Entry<Long, String> pair : topk.entrySet()) {
				context.write(new LongWritable(pair.getKey()),new Text(pair.getValue()));
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

		Job job = Job.getInstance(conf, "TopKclub");
		job.setNumReduceTasks(1);
		job.setJarByClass(Main.class);
		FileSystem fs = FileSystem.get(conf);


		job.setMapperClass(TopKMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BooleanWritable.class);

		job.setReducerClass(TopKReducer.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(CSVLineInputFormat.class);
		CSVLineInputFormat.setInputPaths(job, args[0]);
		job.setOutputFormatClass(TextOutputFormat.class);
		fs.delete(new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
