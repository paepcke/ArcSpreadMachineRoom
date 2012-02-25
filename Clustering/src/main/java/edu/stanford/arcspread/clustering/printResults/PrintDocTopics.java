package edu.stanford.arcspread.clustering.printResults;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.math.VectorWritable;

public class PrintDocTopics 
{
	public static void print(Configuration conf, String input, String output)
	{
		Map<Integer, List<String>> topicMap = createTopicMap(conf, input);
		CSVPrinter.print(conf, output, topicMap);
	}
	
	private static Map<Integer, List<String>> createTopicMap(Configuration conf, String input)
	{
		Map<Integer, List<String>> topicMap = new HashMap<Integer, List<String>>();
		
		try 
		{
			Path file = new Path(input);
			SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.get(conf), file, conf);
			Text doc = new Text();
			VectorWritable vector = new VectorWritable();
			while(reader.next(doc, vector))
			{
				Integer topic = new Integer(vector.get().maxValueIndex());
				if(!topicMap.containsKey(topic))
					topicMap.put(topic, new LinkedList<String>());
				
				String[] docHierarchy = doc.toString().split("/");
				String page = docHierarchy[docHierarchy.length - 1];
				topicMap.get(topic).add(page);
			}
			
			return topicMap;
		} 
		catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}
}
