import java.util.Date;
import java.util.Properties;
import java.util.Scanner;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

public class Main{
	
	static SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MM-yy_HHmm");
	static Date date = new Date();
	static String inputPath;
	static String outputPath;
	static Path tmpPath;
	static Properties prop;
	static Logger logger = Logger.getLogger("Log");  
    static FileHandler fh;  

	static void init() throws IOException {
		prop = new Properties();
		try (FileInputStream input = new FileInputStream("config.properties")) {
			prop.load(input);
			inputPath = prop.getProperty("INPUT_URI");
			outputPath = prop.getProperty("OUTPUT_URI");
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
        Scanner scanner = new Scanner(System.in);
        Configuration conf = new Configuration();
        while(true) {
        	System.out.println("Choose what do you want to calculate:");
    		System.out.println("1 - Top K fastest runners write : topspeed K D=(5, 10, 15, 42, all) G=(MALE,FEMALE, MALE|FEMALE)  ");
    		System.out.println("2 - Top K active clubs write: topclub X=(5, 10, 15, 42, all)");
    		System.out.println("3 - Top K active category: topcat X=(5, 10, 15, 42, all) G=(MALE,FEMALE, MALE|FEMALE)");
    		System.out.println("4 - exit to close.");
    		System.out.print(">>> ");
        	String[] inputs = scanner.nextLine().split(" ");
        	switch (inputs[0]) {
        	case "topspeed":
        		try {
        			conf.setInt("K", Integer.parseInt(inputs[1]));
        			conf.setInt("D", Integer.parseInt(inputs[2]));
        			conf.setStrings("G", new String[]{inputs[3]});
        			TopKspeed topk = new TopKspeed();
        			topk.setConf(conf);
        			System.exit(ToolRunner.run(topk, new String[]{inputPath, outputPath}));
        			break;
        		} catch (Exception e) {
            		System.out.println("Command not valid. please retry");
        		}
        	case "topclub":
        		break;
        	case "topcat" :
        		break;
        	case "exit":
        		return;
        	default:
        		System.out.println("Command not valid. please retry");
        	}
        }
		


		
//		Configuration conf = new Configuration();
//		FileSystem fs = FileSystem.get(conf);
//		
//		Job job = Job.getInstance(conf, "PROJECT");
//		job.setNumReduceTasks(1);
//		job.setJarByClass(Main.class);
//		
//		job.setMapperClass(TopKMapper.class);
//		job.setMapOutputKeyClass(Text.class);
//		job.setMapOutputValueClass(BooleanWritable.class);
//				
//		job.setReducerClass(TopKCombiner.class);
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(Text.class);
//		
//		job.setInputFormatClass(CSVLineInputFormat.class);
//		CSVLineInputFormat.setInputPaths(job, inputPath);
//		job.setOutputFormatClass(TextOutputFormat.class);
//		fs.delete(outputPath);
//		FileOutputFormat.setOutputPath(job, outputPath);
//		
//		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
