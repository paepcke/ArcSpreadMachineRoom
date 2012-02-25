package edu.stanford.arcspread.clustering.printResults;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CSVPrinter 
{
	public static void print(Configuration conf, String output, Map<Integer, List<String>> map)
	{
		try 
		{
			//create the file and get the output stream
			Path outputPath = new Path(output);
			FileSystem fs = FileSystem.get(conf);
			FSDataOutputStream outStream = fs.create(outputPath);
			
			//write the keys as the first row
			LinkedList<Integer> keys = new LinkedList<Integer>(map.keySet());
			for(int i = 0; i < keys.size(); i++)
			{
				Integer key = keys.get(i);
				//don't write a following comma on the last one
				if(i < keys.size() - 1)
					outStream.write((key.toString() + ", ").getBytes());
				else
					outStream.write(key.toString().getBytes());
			}
			outStream.writeChar('\n');
			
			//calculate the number of rows we need to write
			int rows = 0;
			for(Integer key : keys)
			{
				int n = map.get(key).size();
				if(n > rows)
					rows = n;
			}
			
			
			//now write the rows
			for(int i = 0; i < rows; i++)
			{
				for(int j = 0; j < keys.size(); j++)
				{
					Integer k = keys.get(j);
					String val = "";
					if(!map.get(k).isEmpty())
						val = map.get(k).remove(0);
					outStream.write(val.getBytes());
					
					if(j < keys.size() - 1)
						outStream.write(", ".getBytes());
				}
				outStream.writeChar('\n');
			}
			//close the output stream
			outStream.close();
		}
		catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
