import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CSVLineRecordReader extends RecordReader<Text, RunnerWritable> {
	private static final Log LOG = LogFactory.getLog(CSVLineRecordReader.class);
	public static final String DEFAULT_DELIMITER = "\n";
	public static final String DEFAULT_SEPARATOR = ";|,";

	private CompressionCodecFactory compressionCodecs = null;
	private long start;
	private long pos;
	private long end;
	protected BufferedReader in;
	private Text key = null;
	private RunnerWritable value = null;
	private long linesRead;
	private InputStream is;
	private EnumMap<Parameter, MutableBoolean> runnerParam;
	private EnumMap<Parameter, MutableInt> paramPos;
	private String raceName;
	private String year;
	private String distance;

	public CSVLineRecordReader() {
	}

	public CSVLineRecordReader(InputStream is, Configuration conf) throws IOException {
		init(is, conf);
	}
	public void init(InputStream is, Configuration conf) throws IOException {
		this.is = is;
		this.in = new BufferedReader(new InputStreamReader(is));
		linesRead = 0;
	}

	protected int readLine(List<Text> values) throws IOException {
		values.clear();// Empty value columns list
		String line = "";
		int numRead;
		if (in.ready()) {
			line = in.readLine();
		}
		if (line == null) 
			numRead = 0;
		else {
			numRead = line.length();
			linesRead++;
		}

		for (String txt : line.split(DEFAULT_SEPARATOR)) {
			values.add(new Text(txt));
		}
		return numRead;
	}

	public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
		FileSplit split = (FileSplit) genericSplit;
		Configuration job = context.getConfiguration();
		start = split.getStart();
		end = start + split.getLength();
		final Path file = split.getPath();

		Pattern pattern = Pattern.compile("(\\d*)(\\D*)(\\d*)(.csv)");
		Matcher matcher = pattern.matcher( file.getName());
		while(matcher.find()){
			year = matcher.group(1);
			raceName = matcher.group(2);
			distance = matcher.group(3);
		}
		compressionCodecs = new CompressionCodecFactory(job);
		final CompressionCodec codec = compressionCodecs.getCodec(file);

		// open the file and seek to the start of the split
		FileSystem fs = file.getFileSystem(job);
		FSDataInputStream fileIn = fs.open(split.getPath());

		if (codec != null) {
			is = codec.createInputStream(fileIn);
			end = Long.MAX_VALUE;
		} else {
			if (start != 0) {
				fileIn.seek(start);
			}
			is = fileIn;
		}
		this.pos = start;
		init(is, job);
		// Read first line to extract column names
		initColumns();
	}

	private void initColumns() throws IOException {
		List<Text> line = new ArrayList<Text>(0);
		runnerParam = new EnumMap<Parameter, MutableBoolean>(Parameter.class);
		paramPos = new EnumMap<Parameter, MutableInt>(Parameter.class);
		for (Parameter entry : Arrays.asList(Parameter.values())) {
			runnerParam.put(entry, new MutableBoolean(false));
			paramPos.put(entry, new MutableInt(-1));
		}
		pos += readLine(line);
		for (int i = 0; i < line.size(); i++) {
			if (line.get(i).toString().toLowerCase().contains("nom")) {
				runnerParam.get(Parameter.LASTNAME).setValue(true);
				paramPos.get(Parameter.LASTNAME).setValue(i);
			}
			else if (line.get(i).toString().toLowerCase().contains("fff")) {
				runnerParam.get(Parameter.FIRSTNAME).setValue(true);
				paramPos.get(Parameter.FIRSTNAME).setValue(i);
			}
			else if (line.get(i).toString().toLowerCase().matches(".*cat.*")) {
				runnerParam.get(Parameter.CATEGORY).setValue(true);
				paramPos.get(Parameter.CATEGORY).setValue(i);
			}
			else if (line.get(i).toString().toLowerCase().matches(".*temp.*|time|dur|tmp")) {
				runnerParam.get(Parameter.TIME).setValue(true);
				paramPos.get(Parameter.TIME).setValue(i);
			}
			else if (line.get(i).toString().toLowerCase().matches(".*dist.*")) {
				runnerParam.get(Parameter.DISTANCE).setValue(true);
				paramPos.get(Parameter.DISTANCE).setValue(i);
			}
			else if (line.get(i).toString().toLowerCase().matches(".*ran.*")) {
				runnerParam.get(Parameter.RANK).setValue(true);
				paramPos.get(Parameter.RANK).setValue(i);
			}
			else if (line.get(i).toString().toLowerCase().matches(".*club*")) {
				runnerParam.get(Parameter.CLUB).setValue(true);
				paramPos.get(Parameter.CLUB).setValue(i);
			}
		}
	}

	public boolean nextKeyValue() throws IOException {
		if (key == null) {
			key = new Text();
		}
		if (pos >= end)
			return false;
		int newSize = 0;
		List<Text> line = new ArrayList<Text>(0);
		newSize = readLine(line);
		pos += newSize;
		if (newSize == 0) {
			key = null;
			value = null;
			return false;
		} else {
			value = generateRunner(line);
			key.set(this.raceName+linesRead);
			return true;
		}
	}

	private RunnerWritable generateRunner(List<Text> line) {
		RunnerWritable rw = new RunnerWritable();
		try{
			int d = Integer.parseInt(distance);
			rw.setDistance(d);
		}
		catch(Exception e){
			rw.setDistance(-1);
		}

		rw.setRaceName(raceName);

		try{
			rw.setYear(Integer.parseInt(year));
		}
		catch(Exception e){
			rw.setYear(-1);
		}

		int posi;
		String val;
		if (runnerParam.get(Parameter.FIRSTNAME).isTrue()) {
			try {
				posi = paramPos.get(Parameter.FIRSTNAME).intValue();
				val = line.get(posi).toString();
				rw.setFirstname(val);
			} catch (Exception e ) {
				rw.setFirstname("NDF");
			}
		} else {
			rw.setFirstname("NDF");
		}
		if (runnerParam.get(Parameter.LASTNAME).isTrue()) {
			try {
				posi = paramPos.get(Parameter.LASTNAME).intValue();
				val = line.get(posi).toString();
				rw.setLastname(val);
			} catch (Exception e ) {
				rw.setLastname("NDF");
			}
		} else{
			rw.setLastname("NDF");
		}
		if (runnerParam.get(Parameter.CLUB).isTrue()) {
			try {
				posi = paramPos.get(Parameter.CLUB).intValue();
				val = line.get(posi).toString();
				rw.setClubName(val);
			} catch (Exception e ) {
				rw.setClubName("NDF");
			}
		} else {
			rw.setClubName("NDF");
		}

		if (runnerParam.get(Parameter.RANK).isTrue()) {
			try {
				posi = paramPos.get(Parameter.RANK).intValue();
				val = line.get(posi).toString();
				rw.setRank(Integer.parseInt(val));
			} catch (Exception e ) {
				rw.setRank(-1);
			}
		} else {
			rw.setRank(-1);
		}
		if (runnerParam.get(Parameter.TIME).isTrue()) {
			try {
				posi = paramPos.get(Parameter.TIME).intValue();
				val = line.get(posi).toString().toLowerCase();
				long time = 0;
				long hh =0;
				long mm =0;
				long ss=0;
				String val2 = new String(val);
				val2 = val2.replaceAll("[^0-9]+",":");
				String[] tokens = val2.trim().split(":");
				if (tokens.length == 3) {
					hh = Long.parseLong(tokens[0]);
					mm = Long.parseLong(tokens[1]);
					ss = Long.parseLong(tokens[2]);
					time = hh*3600+mm*60+ss;
				} else if (tokens.length == 2) {
					if(val.contains("h")){
						hh = Long.parseLong(tokens[0]);
						mm = Long.parseLong(tokens[1]);
					} 
					else{
						mm = Long.parseLong(tokens[0]);
						ss = Long.parseLong(tokens[1]);
					}
					time = hh*3600+mm*60+ss;
				} else {
					time = -1;
				}
				rw.setTimeInSec(time);
			} catch (Exception e ) {
				rw.setTimeInSec(-1);
			}
		} else {
			rw.setTimeInSec(-1);
		}
		if (runnerParam.get(Parameter.CATEGORY).isTrue()) {
			int rank = rw.getRank();
			Gender gend = Gender.MALE;
			Category cat = Category.OTHER;
			try {
				posi = paramPos.get(Parameter.CATEGORY).intValue();
				val = line.get(posi).toString().toLowerCase();
				if (val.matches("[0-9]+e[ ]+[A-Za-z]+[ ]+[hf]")) {
					String[] tokens = val.split(" ");
					try {
						rank = Integer.parseInt(tokens[0].split("e")[0]);
					} catch (Exception e) {
						rank = -1;
					}

					if (tokens[2].matches("h")) {
						gend = Gender.MALE;
					} else if (tokens[2].matches("f"))
						gend = Gender.FEMALE;
					else {
						gend = Gender.MALE;
					}

					try {
						cat = Category.valueOf(tokens[1].toUpperCase());
					} catch (Exception e) {
						cat = Category.OTHER;
					}
				} else if (val.matches("[0-9]+e[ ]+[A-Za-z]+[ ]+[0-9][ ]+[hf]")) {
					String[] tokens = val.split(" ");
					try {
						rank = Integer.parseInt(tokens[0].split("e")[0]);
					} catch (Exception e) {
						rank = -1;
					}
					if (tokens[3].matches("h")) {
						gend = Gender.MALE;
					} else if (tokens[3].matches("f"))
						gend = Gender.FEMALE;
					else {
						gend = Gender.MALE;
					}
					try {
						cat = Category.valueOf(tokens[1].toUpperCase());
					} catch (Exception e) {
						cat = Category.OTHER;
					}
				} else {
					cat = Category.OTHER;
					gend = Gender.MALE;
				}
				rw.setCategory(cat);
				rw.setGender(gend);
				rw.setRank(rank);
			} catch (Exception e) {
				rw.setCategory(Category.OTHER);
				rw.setGender(Gender.MALE);
				rw.setRank(rank);
			}
		} else {
			rw.setCategory(Category.OTHER);
			rw.setGender(Gender.MALE);
			rw.setRank(rw.getRank());
		}
		return rw;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapreduce.RecordReader#getCurrentKey()
	 */
	@Override
	public Text getCurrentKey() {
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