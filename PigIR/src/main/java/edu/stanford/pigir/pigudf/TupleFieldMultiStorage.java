package edu.stanford.pigir.pigudf;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.pig.StoreFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.util.StorageUtil;

/**
 * The UDF stores a field of a tuple into its own file.  A field of the tuple is also used 
 * to name the file.
 * 
 * Sample usage: <code>
 * A = LOAD 'mydata' USING PigStorage() as (a, b, c);
 * STORE A INTO '/my/home/output' USING MultiStorage('/my/home/output','0', '1');
 * </code> Parameter details: <b>/my/home/output </b>(Required) :
 * The DFS path where output directories and files will be created. <b> 0
 * </b>(Required) : Index of field whose values should be used to name the files
 * <b>'1' </b>(Required) : Index of field whose values are stored into the file.
 * 
 */
public class TupleFieldMultiStorage extends StoreFunc {

  @SuppressWarnings("unused")
  private Path outputPath; // User specified output Path
  private int nameFieldIndex = -1; // Index of the key field
  private int writeFieldIndex = -1; // Index of the value field that is written into the document

  /**
   * Constructor
   * 
   * @param parentPathStr
   *          Parent output dir path
   * @param nameFieldIndex
   *          field index used to name the files
   * @param writeFieldIndex
   *          field index that gets stored into the file
   * 
   */
  public TupleFieldMultiStorage(String parentPathStr, String nameFieldIndex, String writeFieldIndex) 
  {
    this.outputPath = new Path(parentPathStr);
    this.nameFieldIndex = Integer.parseInt(nameFieldIndex);
    this.writeFieldIndex = Integer.parseInt(writeFieldIndex);
  }

  //--------------------------------------------------------------------------
  // Implementation of StoreFunc

  private RecordWriter<String, Tuple> writer;
  
  @Override
  public void putNext(Tuple tuple) throws IOException 
  {
	if (tuple.size() <= nameFieldIndex) 
	{
	  throw new IOException("split field index:" + this.nameFieldIndex
	  + " >= tuple size:" + tuple.size());
	}
	
	if(tuple.size() <= writeFieldIndex)
	{
		throw new IOException("write field index:" + writeFieldIndex 
			+ " >= tuple size: " + tuple.size());
	}
	
	Object field = null;
	Object docContent = null;
	try 
	{
	  field = tuple.get(nameFieldIndex);
	  docContent = tuple.get(writeFieldIndex);
	  
	  Tuple writeTuple = TupleFactory.getInstance().newTuple(docContent);
	  writer.write(String.valueOf(field), writeTuple);      
	} 
	catch (ExecException exec) 
	{
		throw new IOException(exec);
	} 
	catch (InterruptedException e) 
	{
		throw new IOException(e);
	}
  }
  
  @SuppressWarnings("rawtypes")
  @Override
  public OutputFormat getOutputFormat() throws IOException 
  {
      MultiStorageOutputFormat format = new MultiStorageOutputFormat();
      return format;
  }
    
  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Override
  public void prepareToWrite(RecordWriter writer) throws IOException 
  {
      this.writer = writer;
  }
    
  @Override
  public void setStoreLocation(String location, Job job) throws IOException 
  {
    job.getConfiguration().set("mapred.textoutputformat.separator", "");
    FileOutputFormat.setOutputPath(job, new Path(location));
  }
 
  //--------------------------------------------------------------------------
  // Implementation of OutputFormat
  
  public static class MultiStorageOutputFormat extends
  TextOutputFormat<String, Tuple> 
  {

    private String keyValueSeparator = "\\t";
    private byte fieldDel = '\t';
  
    @Override
    public RecordWriter<String, Tuple> 
    getRecordWriter(TaskAttemptContext context
                ) throws IOException, InterruptedException 
    {
    
      final TaskAttemptContext ctx = context;
        
      return new RecordWriter<String, Tuple>() 
	  {
        private Map<String, MyLineRecordWriter> storeMap = 
              new HashMap<String, MyLineRecordWriter>();
          
        private static final int BUFFER_SIZE = 1024;
          
        private ByteArrayOutputStream mOut = 
              new ByteArrayOutputStream(BUFFER_SIZE);
                           
        @Override
        public void write(String key, Tuple val) throws IOException 
        {
          int sz = val.size();
          for (int i = 0; i < sz; i++) {
            Object field;
            try {
              field = val.get(i);
            } catch (ExecException ee) {
              throw ee;
            }

            StorageUtil.putField(mOut, field);

            if (i != sz - 1) {
              mOut.write(fieldDel);
            }
          }
              
          getStore(key).write(null, new Text(mOut.toByteArray()));

          mOut.reset();
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException { 
          for (MyLineRecordWriter out : storeMap.values()) {
            out.close(context);
          }
        }
      
        private MyLineRecordWriter getStore(String fieldValue) throws IOException {
          MyLineRecordWriter store = storeMap.get(fieldValue);
          if (store == null) {                  
            DataOutputStream os = createOutputStream(fieldValue);
            store = new MyLineRecordWriter(os, keyValueSeparator);
            storeMap.put(fieldValue, store);
          }
          return store;
        }
          
        private DataOutputStream createOutputStream(String fieldValue) throws IOException {
          Configuration conf = ctx.getConfiguration();
          TaskID taskId = ctx.getTaskAttemptID().getTaskID();
          
          // Check whether compression is enabled, if so get the extension and add them to the path
          boolean isCompressed = getCompressOutput(ctx);
          CompressionCodec codec = null;
          String extension = "";
          if (isCompressed) {
             Class<? extends CompressionCodec> codecClass = 
                getOutputCompressorClass(ctx, GzipCodec.class);
             codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, ctx.getConfiguration());
             extension = codec.getDefaultExtension();
          }
          
          Path path = new Path(fieldValue + '-'
                + NumberFormat.getInstance().format(taskId.getId())+extension);
          Path workOutputPath = ((FileOutputCommitter)getOutputCommitter(ctx)).getWorkPath();
          Path file = new Path(workOutputPath, path);
          FileSystem fs = file.getFileSystem(conf);                
          FSDataOutputStream fileOut = fs.create(file, false);
          
          if (isCompressed)
             return new DataOutputStream(codec.createOutputStream(fileOut));
          else
             return fileOut;
        }
          
      };
    }
  
    public void setKeyValueSeparator(String sep) {
      keyValueSeparator = sep;
      fieldDel = StorageUtil.parseFieldDel(keyValueSeparator);  
    }
  
  //------------------------------------------------------------------------
  //
  
    @SuppressWarnings("rawtypes")
	protected static class MyLineRecordWriter
    extends TextOutputFormat.LineRecordWriter<WritableComparable, Text> {

      public MyLineRecordWriter(DataOutputStream out, String keyValueSeparator) {
        super(out, keyValueSeparator);
      }
    }
  }

}
