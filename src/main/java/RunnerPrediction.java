import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

public class RunnerPrediction extends Configured implements Tool {


	public static class RunnerResume implements Writable, Cloneable {

		long min, max, count, sum;
		public RunnerResume() {}

		public RunnerResume(long pop) {
			sum = max = min = pop;
			count = 1;
		}

		public RunnerResume clone() {
			try {
				return (RunnerResume)super.clone();
			}
			catch (Exception e) {
				System.err.println(e.getStackTrace());
				System.exit(-1);
			}
			return null;
		}

		void merge(RunnerResume Runner) {
			min = Math.min(min, Runner.min);
			max = Math.max(max, Runner.max);
			count += Runner.count;
			sum += Runner.sum;
		}

		public void write(DataOutput out) throws IOException {
			out.writeLong(min);
			out.writeLong(max);
			out.writeLong(sum);
			out.writeLong(count);
		}

		public void readFields(DataInput in) throws IOException {
			min   = in.readLong();
			max   = in.readLong();
			sum   = in.readLong();
			count = in.readLong();
		}

		public String toString() {
			StringBuilder tmp = new StringBuilder();
			tmp.append(count);
			tmp.append(",");
			tmp.append(min);
			tmp.append(",");
			tmp.append(max);
			tmp.append(",");
			tmp.append((double)sum/(double)count);
			return tmp.toString();
		}
	}

	private static class ResumeMapper extends Mapper<Text, RunnerWritable, IntWritable, RunnerResume> {
		private int sec_step = 0;
		private int distance;
		private String gender;
		private String category;

		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			sec_step = conf.getInt("step", 10);
			distance = conf.getInt("D", 10);
			gender = conf.getStrings("G", new String[]{"MALE|FEMALE"})[0];
			category = conf.getStrings("C", new String[]{"ALL"})[0];
		}

		public void map(Text key, RunnerWritable value, Context context) throws IOException, InterruptedException {
			int d = value.getDistance();
			String gend = value.getGender().toString();
			String cat = value.getCategory().toString();
			long time = value.getTimeInSec();
			boolean bool;
			if (category.equals("ALL"))
				bool = true;
			else 
				bool = cat.contains(category);
			if (time>0&&gend.matches(gender) &&bool && d == distance) {
				double speed = (distance*1000.0) / (time*1.0) ;
				if(speed>6)
					return;
				int bin = (int)Math.floor(time/sec_step)*sec_step;
				context.write(new IntWritable(bin), new RunnerResume(time));
			}
		}
	}

	public static class ResumeCombiner extends Reducer<IntWritable, RunnerResume, IntWritable, RunnerResume> {
		public static RunnerResume merge(Iterable<RunnerResume> values) {
			RunnerResume resume = null;
			Iterator<RunnerResume> it= values.iterator();
			if (it.hasNext())  {
				resume = it.next().clone();
				while(it.hasNext()) resume.merge(it.next());
			}
			return resume;
		}

		public void reduce(IntWritable key, Iterable<RunnerResume> values,Context context) throws IOException, InterruptedException {
			RunnerResume resume = merge(values);
			if (resume != null) context.write(key, resume);
		}
	}

	public static class ResumeReducer   extends Reducer<IntWritable, RunnerResume, Text, Text> {
		public int nb_step = 0;
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			nb_step = conf.getInt("steps", 10);
		}
		public void reduce(IntWritable key, Iterable<RunnerResume> values,Context context) throws IOException, InterruptedException {
			RunnerResume resume = ResumeCombiner.merge(values);
			context.write(new Text(getTimeInFormat(key.get())+"-"+getTimeInFormat(key.get()+nb_step)), new Text(resume.toString()));
		}
		
		private String getTimeInFormat(int timeInSec) {
			String mm = String.valueOf(timeInSec/60);
			String ss = String.valueOf(timeInSec%60);
			return mm+"m"+ss+"s";
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

		Job job = Job.getInstance(conf, "RunnerPrediction");
		job.setNumReduceTasks(1);
		job.setJarByClass(Main.class);
		FileSystem fs = FileSystem.get(conf);


		job.setMapperClass(ResumeMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(RunnerResume.class);
		
		job.setCombinerClass(ResumeCombiner.class);
		job.setReducerClass(ResumeReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(CSVLineInputFormat.class);
		CSVLineInputFormat.setInputPaths(job, args[0]);
		job.setOutputFormatClass(TextOutputFormat.class);
		fs.delete(new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

}
