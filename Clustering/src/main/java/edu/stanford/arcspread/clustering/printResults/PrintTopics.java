package edu.stanford.arcspread.clustering.printResults;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.mahout.common.IntPairWritable;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterable;
import org.apache.mahout.utils.vectors.VectorHelper;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class PrintTopics 
{
	public static void print(Configuration conf, String input, String dictFile, String output)
	{
		Map<Integer, List<String>> topics = createTopicMap(conf, input, dictFile);
		CSVPrinter.print(conf, output, topics);
	}
	
	private static Map<Integer, List<String>> createTopicMap(Configuration conf, String input, String dictFile)
	{
		int numWords = 20; //TODO make this a parameter? 
		List<String> wordList = Arrays.asList(VectorHelper.loadTermDictionary(conf, dictFile));
		List<Queue<Pair<String,Double>>> topWords = topWordsForTopics(input, conf, wordList, numWords);
		
		Map<Integer, List<String>> topicMap = new HashMap<Integer, List<String>>();
		for(int i = 0; i < topWords.size(); i++)
		{
			topicMap.put(new Integer(i), new LinkedList<String>());
			Collection<Pair<String, Double>> topicWords = topWords.get(i);
			
			//add all the words to the topic map
			for(Pair<String, Double> word : topicWords)
			{
				topicMap.get(i).add(word.getFirst());
			}			
		}
		
		return topicMap;
	}
	
	private static List<Queue<Pair<String,Double>>> topWordsForTopics(String dir,
            Configuration job,
            List<String> wordList,
            int numWordsToPrint) 
    {
		List<Queue<Pair<String,Double>>> queues = Lists.newArrayList();
	    Map<Integer,Double> expSums = Maps.newHashMap();
	    for (Pair<IntPairWritable,DoubleWritable> record :
	         new SequenceFileDirIterable<IntPairWritable, DoubleWritable>(
	             new Path(dir, "part-*"), PathType.GLOB, null, null, true, job)) {
	      IntPairWritable key = record.getFirst();
	      int topic = key.getFirst();
	      int word = key.getSecond();
	      ensureQueueSize(queues, topic);
	      if (word >= 0 && topic >= 0) {
	        double score = record.getSecond().get();
	        if (expSums.get(topic) == null) {
	          expSums.put(topic, 0.0);
	        }
	        expSums.put(topic, expSums.get(topic) + Math.exp(score));
	        String realWord = wordList.get(word);
	        maybeEnqueue(queues.get(topic), realWord, score, numWordsToPrint);
	      }
	    }
	    for (int i = 0; i < queues.size(); i++) {
	      Queue<Pair<String,Double>> queue = queues.get(i);
	      Queue<Pair<String,Double>> newQueue = new PriorityQueue<Pair<String, Double>>(queue.size());
	      double norm = expSums.get(i);
	      for (Pair<String,Double> pair : queue) {
	        newQueue.add(new Pair<String,Double>(pair.getFirst(), Math.exp(pair.getSecond()) / norm));
	      }
	      queues.set(i, newQueue);
	    }
	    return queues;
    }
	
	// Expands the queue list to have a Queue for topic K
	private static void ensureQueueSize(Collection<Queue<Pair<String,Double>>> queues, int k) {
	  for (int i = queues.size(); i <= k; ++i) {
	    queues.add(new PriorityQueue<Pair<String,Double>>());
	  }
	}
	
	// Adds the word if the queue is below capacity, or the score is high enough
	private static void maybeEnqueue(Queue<Pair<String,Double>> q, String word, double score, int numWordsToPrint) {
	  if (q.size() >= numWordsToPrint && score > q.peek().getSecond()) {
	    q.poll();
	  }
	  if (q.size() < numWordsToPrint) {
	    q.add(new Pair<String,Double>(word, score));
	  }
	}
}
