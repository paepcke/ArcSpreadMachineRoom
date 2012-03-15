package edu.stanford.pigir.warc;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;

import edu.stanford.pigir.pigudf.LineAndChunkReader;

/**
 * Treats keys as offset wbRecordReader file and value as one Warc record. 
 */
public class WarcRecordReader extends RecordReader<LongWritable, Text> {
	
  public static final boolean DO_READ_CONTENT = true;
  private static final int DEFAULT_BUFFER_SIZE = 64 * 1024;
  private final Logger logger = Logger.getLogger(WarcLoader.class.getName());
  private long start;
  private long pos;
  private long end;
  private LineAndChunkReader warcLineReader;
  private DataInputStream warcInStream; 
  private LongWritable keyWarcStreamPos = null;
  private WarcRecord valueWarcRecord = null;
  private FSDataInputStream fileIn = null;
  private FileInputStream localFileIn = null;

  
public void initialize(File warcFile) throws IOException {
	org.apache.hadoop.fs.Path warcFilePath = new org.apache.hadoop.fs.Path(warcFile.getAbsolutePath());
	long fileStart  = 0L;
	long fileLength = warcFile.getTotalSpace(); // ????
	String[] hosts  = new String[1];
	FileSplit inputSplit = new FileSplit(warcFilePath, 
			fileStart,
			fileLength,
			hosts);
	initialize(inputSplit, null);
}
    
/* (non-Javadoc)
 * @see org.apache.hadoop.mapreduce.RecordReader#initialize(org.apache.hadoop.mapreduce.InputSplit, org.apache.hadoop.mapreduce.TaskAttemptContext)
 * If using WARC reader outside of a Hadoop framework, use initialize(File), not
 * this method. initialize(File) will pass null for the context, which we handle
 * in this method.
 */
public void initialize(InputSplit genericSplit,
                         TaskAttemptContext context) throws IOException {
    FileSplit split = (FileSplit) genericSplit;
    Configuration job = null;
    if (context != null)
    	job = context.getConfiguration();
    else {
    	// Using WARC reader outside of a Hadoop context: 
    	job = new Configuration();
    	job.set("io.file.buffer.size", "4096");
    }
    start = split.getStart();
    end = start + split.getLength();
    final Path file = split.getPath();
    FileSystem fs = null;
    
    if (context != null) {
    	fs = file.getFileSystem(job);
    	fileIn = fs.open(split.getPath());
    }
    else {
    	// Using WARC reader outside of a Hadoop context:
    	String absoluteFileName = split.getPath().toString();
    	localFileIn = new FileInputStream(absoluteFileName);
    }

    GZIPInputStream gzWarcInStream = null;
    try {
    	if (context != null)
    		gzWarcInStream = new GZIPInputStream(fileIn,
    											 job.getInt("io.file.buffer.size", DEFAULT_BUFFER_SIZE));
    	else
    		// Using WARC reader outside of a Hadoop context: 
    		gzWarcInStream = new GZIPInputStream(localFileIn);
    	
    	warcInStream = new DataInputStream(gzWarcInStream);
    } catch (IOException e) {
    	// Not a gzipped file?
    	try {
    		// The attempt to read using the gzip stream above consumed
    		// the first two bytes. Reset:
    		fileIn.seek(0);
    		warcInStream = new DataInputStream (fileIn);
    	} catch (Exception e1) {
    		logger.info("Could not open WARC split.");
    		return;
    	}
    }
    // TODO: Test slices with non-zero start, and with start > MAX_INTEGER
    boolean skipFirstLine = false;
    if (start != 0) {
    	skipFirstLine = true;
    	--start;
    	// Skipping wbRecordReader a datastream only works for int. So, if
    	// start within the slice is greater than what fits into 
    	// an int, we need to skip wbRecordReader multiple steps:
    	if (start > Integer.MAX_VALUE) {
    		for (int intChunkCount=0; intChunkCount < (start / Integer.MAX_VALUE); intChunkCount++) {
    			warcInStream.skipBytes(Integer.MAX_VALUE);
    		}
    		warcInStream.skipBytes(((Long)(start % (new Long(Integer.MAX_VALUE)))).intValue());
    	}
    	warcInStream.skipBytes((int) (start - pos));
    }
    warcLineReader = new LineAndChunkReader(warcInStream, job);

    if (skipFirstLine) {  // skip first line and re-establish "start".
      start += warcLineReader.readLine(new Text(), 0,
                           (int)Math.min((long)Integer.MAX_VALUE, end - start));
    }
    this.pos = start;
  }
  
  
  public boolean nextKeyValue() throws IOException {
	  return nextKeyValue(DO_READ_CONTENT);
  }
  
  public boolean nextKeyValue(boolean readContents) throws IOException {
    if (keyWarcStreamPos == null) {
      keyWarcStreamPos = new LongWritable();
    }
    keyWarcStreamPos.set(pos);
    valueWarcRecord = WarcRecord.readNextWarcRecord(warcLineReader, readContents);
    if (valueWarcRecord == null) {
    	keyWarcStreamPos = null;
    	return false;
    }
    
    logger.debug("Pulled another WARC record.");
    
    // Update position wbRecordReader the Data stream
    pos += valueWarcRecord.getTotalRecordLength();
    return true;
  }

  @Override
  public LongWritable getCurrentKey() {
    return keyWarcStreamPos;
  }

  @Override
  public WarcRecord getCurrentValue() {
    return valueWarcRecord;
  }

  /**
   * Get the progress within the split
   */
  public float getProgress() {
    if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (pos - start) / (float)(end - start));
    }
  }
  
  public synchronized void close() throws IOException {
    if (warcLineReader	 != null) {
      warcLineReader.close(); 
    }
  }
  
	// ---------------------------------------- Inner Class WarcFileInputStream ----------------------------
	
	//class WarcFileInputStream extends FSDataInputStream implements PositionedReadable, Seekable {
  	class WarcFileInputStream extends FileInputStream implements PositionedReadable, Seekable {
		
		FileInputStream warcFileStream = null;

		public WarcFileInputStream(String warcFilePathStr) throws IOException {
			this(new File(warcFilePathStr));
		}
		
		public WarcFileInputStream(File warcFileObj) throws IOException {
			super(warcFileObj);
			warcFileStream = new FileInputStream(warcFileObj); 
		}

		public int read(long position, byte[] buffer, int offset, int length)
				throws IOException {
			warcFileStream.skip(position);
			mark(length + 1);
			int bytesRead = warcFileStream.read(buffer, offset, length);
			warcFileStream.reset();
			return bytesRead;
		}

		public void readFully(long position, byte[] buffer) throws IOException {
			read(position, buffer, 0, buffer.length);
		}

		public void readFully(long position, byte[] buffer, int offset, int length)
				throws IOException {
			read(position, buffer, offset, length);
		}

		public long getPos() throws IOException {
			throw new IOException("Method getPos() not implemented for local Warc file reading.");
		}

		public void seek(long numBytes) throws IOException {
			throw new IOException("Method seek() not implemented for local Warc file reading.");
		}

		public boolean seekToNewSource(long arg0) throws IOException {
			return false;
		}
	}
}

