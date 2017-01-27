import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Pattern;
import java.util.zip.ZipInputStream;

public class CSVLineRecordReader extends RecordReader<LongWritable, RunnerWritable> {
	public static final String DEFAULT_DELIMITER = "\"";
	public static final String DEFAULT_SEPARATOR = ",";
	
	private long start;
	private long pos;
	private long end;
	protected Reader in;
	private LongWritable key = null;
	private RunnerWritable value = null;
	private String delimiter;
	private String separator;
	private InputStream is;
	private EnumMap<Parameter, MutableBoolean> runnerParam;
	private EnumMap<Parameter, MutableInt> paramPos;

	public CSVLineRecordReader() {
	}

	/**
	 * Constructor to be called from FileInputFormat.createRecordReader
	 * 
	 * @param is
	 *            - the input stream
	 * @param conf
	 *            - hadoop conf
	 * @throws IOException
	 */
	public CSVLineRecordReader(InputStream is, Configuration conf) throws IOException {
		init(is, conf);
	}
	
	/**
	 * reads configuration set in the runner, setting delimiter and separator to
	 * be used to process the CSV file .
	 * 
	 * @param is
	 *            - the input stream
	 * @param conf
	 *            - hadoop conf
	 * @throws IOException
	 */
	public void init(InputStream is, Configuration conf) throws IOException {
		this.delimiter =  DEFAULT_DELIMITER;
		this.separator = DEFAULT_SEPARATOR;
		this.is = is;
		this.in = new BufferedReader(new InputStreamReader(is));
	}

	/**
	 * Parses a line from the CSV, from the current stream position. It stops
	 * parsing when it finds a new line char outside two delimiters
	 * 
	 * @param values
	 *            List of column values parsed from the current CSV line
	 * @return number of chars processed from the stream
	 * @throws IOException
	 */
	protected int readLine(List<Text> values) throws IOException {
		values.clear();// Empty value columns list
		char c;
		int numRead = 0;
		boolean insideQuote = false;
		StringBuffer sb = new StringBuffer();
		int i;
		int quoteOffset = 0, delimiterOffset = 0;
		// Reads each char from input stream unless eof was reached
		while ((i = in.read()) != -1) {
			c = (char) i;
			numRead++;
			sb.append(c);
			// Check quotes, as delimiter inside quotes don't count
			if (c == delimiter.charAt(quoteOffset)) {
				quoteOffset++;
				if (quoteOffset >= delimiter.length()) {
					insideQuote = !insideQuote;
					quoteOffset = 0;
				}
			} else {
				quoteOffset = 0;
			}
			// Check delimiters, but only those outside of quotes
			if (!insideQuote) {
				if (c == separator.charAt(delimiterOffset)) {
					delimiterOffset++;
					if (delimiterOffset >= separator.length()) {
						foundDelimiter(sb, values, true);
						delimiterOffset = 0;
					}
				} else {
					delimiterOffset = 0;
				}
				// A new line outside of a quote is a real csv line breaker
				if (c == '\n') {
					break;
				}
			}
		}
		foundDelimiter(sb, values, false);
		return numRead;
	}

	/**
	 * Helper function that adds a new value to the values list passed as
	 * argument.
	 * 
	 * @param sb
	 *            StringBuffer that has the value to be added
	 * @param values
	 *            values list
	 * @param takeDelimiterOut
	 *            should be true when called in the middle of the line, when a
	 *            delimiter was found, and false when sb contains the line
	 *            ending
	 * @throws UnsupportedEncodingException
	 */
	protected void foundDelimiter(StringBuffer sb, List<Text> values, boolean takeDelimiterOut)
			throws UnsupportedEncodingException {

        //remove trailing LF
        if (sb.length() > 0 && sb.charAt(sb.length()-1) == '\n'){
            sb.deleteCharAt(sb.length()-1);
        }

		// Found a real delimiter
		Text text = new Text();
		String val = (takeDelimiterOut) ? sb.substring(0, sb.length() - separator.length()) : sb.toString();
		if (val.startsWith(delimiter) && val.endsWith(delimiter)) {
			val = (val.length() - (2 * delimiter.length()) > 0) ? val.substring(delimiter.length(), val.length()
					-  delimiter.length()) : "";
		}
		text.append(val.getBytes("UTF-8"), 0, val.length());
		values.add(text);
		// Empty string buffer
		sb.setLength(0);
	}

	
	public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
		FileSplit split = (FileSplit) genericSplit;
		Configuration job = context.getConfiguration();

		start = split.getStart();
		end = start + split.getLength();
		final Path file = split.getPath();

		// open the file and seek to the start of the split
		FileSystem fs = file.getFileSystem(job);
		FSDataInputStream fileIn = fs.open(split.getPath());
		if (start != 0) {
			fileIn.seek(start);
		}
		init(is, job);
		
		//Read first line to extract column names
		initColumns();
	}

	private void initColumns() throws IOException {
		List<Text> line = new ArrayList<Text>(0);
		for (MutableBoolean entry : runnerParam.values()) {
			entry.setValue(false);
		}
		readLine(line);
		for (int i = 0; i < line.size(); i++) {
			 if (line.get(i).toString().toLowerCase().matches("name|nom|last name")) {
				 runnerParam.get(Parameter.LASTNAME).setValue(true);
				 paramPos.get(Parameter.LASTNAME).setValue(i);
			 }
			 if (line.get(i).toString().toLowerCase().matches("first name|prénom|prenom")) {
				 runnerParam.get(Parameter.FIRSTNAME).setValue(true);
				 paramPos.get(Parameter.FIRSTNAME).setValue(i);
			 }
			 if (line.get(i).toString().toLowerCase().matches(".*categ.*")) {
				 runnerParam.get(Parameter.CATEGORY).setValue(true);
				 paramPos.get(Parameter.CATEGORY).setValue(i);
			 }
			 if (line.get(i).toString().toLowerCase().matches(".*temp.*|time|last name")) {
				 runnerParam.get(Parameter.TIME).setValue(true);
				 paramPos.get(Parameter.TIME).setValue(i);
			 }
			 if (line.get(i).toString().toLowerCase().matches(".*dist.*")) {
				 runnerParam.get(Parameter.DISTANCE).setValue(true);
				 paramPos.get(Parameter.DISTANCE).setValue(i);
			 }
			 if (line.get(i).toString().toLowerCase().matches(".*ran.*")) {
				 runnerParam.get(Parameter.RANK).setValue(true);
				 paramPos.get(Parameter.RANK).setValue(i);
			 }
		}
	}


	public boolean nextKeyValue() throws IOException {
		if (key == null) {
			key = new LongWritable();
		}
		key.set(pos);

		if (value == null) {
			value = new RunnerWritable();
		}
		while (true) {
			if (pos >= end)
				return false;
			int newSize = 0;
			List<Text> line = new ArrayList<Text>(0);
			newSize = readLine(line);
			populateRunner(line,value);
			pos += newSize;
			if (newSize == 0) {
				key = null;
				value = null;
				return false;
			} else {
				return true;
			}
		}
	}

	private void populateRunner(List<Text> line, RunnerWritable value2) {
		
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapreduce.RecordReader#getCurrentKey()
	 */
	@Override
	public LongWritable getCurrentKey() {
		return key;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapreduce.RecordReader#getCurrentValue()
	 */
	@Override
	public RunnerWritable getCurrentValue() {
		return value;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapreduce.RecordReader#getProgress()
	 */
	public float getProgress() {
		if (start == end) {
			return 0.0f;
		} else {
			return Math.min(1.0f, (pos - start) / (float) (end - start));
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapreduce.RecordReader#close()
	 */
	public synchronized void close() throws IOException {
		if (in != null) {
			in.close();
			in = null;
		}
		if (is != null) {
			is.close();
			is = null;
		}
	}
}