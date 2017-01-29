import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class CSVFileSplit extends FileSplit {
	private long linesCount;
	private long offset;
	
	public CSVFileSplit(Path fileName, long begin, long l, String[] strings) {
		super(fileName, begin, l, strings);
	}
	
	public long getOffset() {
		return offset;
	}
	public void setOffset(long offset) {
		this.offset = offset;
	}
	public long getLinesCount() {
		return linesCount;
	}
	public void setLinesCount(long linesCount) {
		this.linesCount = linesCount;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeLong(linesCount);
		out.writeLong(offset);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		linesCount = in.readLong();
		offset =  in.readLong();
	}

}
