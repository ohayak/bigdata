import java.util.Date;
import java.util.Properties;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
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
	static Logger logger = Logger.getLogger("Log");  
    static FileHandler fh;  

	static void init() throws IOException {
		prop = new Properties();
		try (FileInputStream input = new FileInputStream("config.properties")) {
			prop.load(input);
			inputPath = new Path(prop.getProperty("INPUT_URI"));
			outputPath = new Path(prop.getProperty("OUTPUT_URI"));
			tmpPath = new Path(prop.getProperty("TMP_URI"));
		}
		try {
	        // This block configure the logger with handler and formatter  
	        fh = new FileHandler("mapreduce.log");  
	        logger.addHandler(fh);
	        SimpleFormatter formatter = new SimpleFormatter();  
	        fh.setFormatter(formatter); 
	        logger.setUseParentHandlers(false);
	    } catch (SecurityException e) {  
	        e.printStackTrace();  
	    } catch (IOException e) {  
	        e.printStackTrace();  
	    }
	}
	
	public static void main(String[] args) throws Exception {
		
		init();
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		Job job = Job.getInstance(conf, "PROJECT");
		job.setNumReduceTasks(1);
		job.setJarByClass(Main.class);
		
		job.setMapperClass(TPMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(TPReducer.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(CSVLineInputFormat.class);
		CSVLineInputFormat.setInputPaths(job, inputPath);
		CSVLineInputFormat.setNumLinesPerSplit(job, 10000);
		job.setOutputFormatClass(TextOutputFormat.class);
		fs.delete(outputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
	
	
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
