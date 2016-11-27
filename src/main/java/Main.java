import java.util.Date;
import java.util.Properties;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.fs.FileSystem;

public class Main{
	
	static SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MM-yy_HHmm");
	static Date date = new Date();
	static Path inputPath;
	static Path outputPath;
	static Path tmpPath;
	static Properties prop;
	
	static void init() throws IOException {
		prop = new Properties();
		try (FileInputStream input = new FileInputStream("config.properties")) {
			prop.load(input);
			inputPath = new Path(prop.getProperty("INPUT_URI"));
			outputPath = new Path(prop.getProperty("OUTPUT_URI"));
			tmpPath = new Path(prop.getProperty("TMP_URI"));
		}
	}
	
	public static void main(String[] args) throws Exception {
		
		init();
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		Job job = Job.getInstance(conf, "TP4");
		conf.setLong("split_length", Long.parseLong(prop.getProperty("SPLIT_LENGTH")));
		conf.setLong("split_number",Long.parseLong(prop.getProperty("SPLIT_NUMBER")));
		job.setNumReduceTasks(1);
		job.setJarByClass(Main.class);
		
		job.setMapperClass(TPMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BooleanWritable.class);
		
		job.setReducerClass(TPReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		fs.delete(outputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		job.setInputFormatClass(RandomPointInputFormat.class);
	
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
