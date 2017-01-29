import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class CSVLineInputFormat extends FileInputFormat<Text, RunnerWritable> {
	
	@Override
	public RecordReader<Text, RunnerWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		String delimeter = CSVLineRecordReader.DEFAULT_DELIMITER;
		String separator = CSVLineRecordReader.DEFAULT_SEPARATOR;
		if (null == delimeter || null == separator) {
			throw new IOException("CSVLineInputFormat: missing parameter delimiter/separator");
		}
		context.setStatus(split.toString());
		return new CSVLineRecordReader();
	}

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException {
		List<InputSplit> splits = new ArrayList<InputSplit>();
		// Take input file (a directory containing csv files) and create even splits
		for (FileStatus status : listStatus(context)) {
			FileSplit fileSplit = getSplitFromFile(status, context.getConfiguration());
			splits.add(fileSplit);
		}
		return splits;
	}
	
	public static FileSplit getSplitFromFile(FileStatus status, Configuration conf)
			throws IOException {
		Path fileName = status.getPath();
		if (status.isDir()) {
			throw new IOException("Not a file: " + fileName);
		}
		return new FileSplit(fileName, 0, status.getLen(), new String[] {});
	}

	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		return true;
	}
}
