import java.util.Date;
import java.util.Properties;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.fs.FileSystem;

public class TP3 {
	
	static SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MM-yy_HHmm");
	static Date date = new Date();
	static String inputPath;
	static String outputPath;
	static String tmpPath;
	
	static void init() throws FileNotFoundException, IOException {
		Properties prop = new Properties();
		try (FileInputStream input = new FileInputStream("config.properties")) {
			prop.load(input);
			inputPath = prop.getProperty("INPUT_URI");
			outputPath = prop.getProperty("OUTPUT_URI");
			tmpPath = prop.getProperty("TMP_URI");
		}
	}
	
	public static void main(String[] args) throws Exception {
		
		init();
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		Job job = Job.getInstance(conf, "TP3");
		job.setNumReduceTasks(1);
		job.setJarByClass(TP3.class);
		job.setMapperClass(TP3Mapper1.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setReducerClass(TP3Reducer1.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(tmpPath));
		job.waitForCompletion(true);
		
		Job job2 = Job.getInstance(conf, "TP3");
		job2.setNumReduceTasks(1);
		job2.setJarByClass(TP3.class);
		job2.setMapperClass(TP3Mapper2.class);
		job2.setMapOutputKeyClass(LongWritable.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setReducerClass(TP3Reducer2.class);
		job2.setOutputKeyClass(LongWritable.class);
		job2.setOutputValueClass(LongWritable.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		job2.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job2, new Path(tmpPath));
		FileOutputFormat.setOutputPath(job2, new Path(outputPath+"_"+dateFormat.format(date)));
		fs.deleteOnExit(new Path(tmpPath));
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}
