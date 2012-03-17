package edu.stanford.pigir.pigudf;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

//import edu.stanford.pigir.arcspread.Stopwords;

public class FilterClusteringStopwords extends EvalFunc<String> 
{
	private List<String> stopwords = new LinkedList<String>();
		
	public FilterClusteringStopwords()
	{
		//add clustering specific stopwords / stop phrases here		
		stopwords.add("The Augusta Chronicle");
		stopwords.add("2theadvocate.com");
	}
	
	@Override
	public String exec(Tuple input) throws IOException 
	{
		if(input.size() == 0)
			return null;
		
		//replace all occurences of stopwords with an empy string
		String str = (String) input.get(0);
		for(String sw : stopwords)
			str = str.replace(sw, "");
				
//		for(String sw : Stopwords.getStopwords())
//			str = str.replace(sw, "");
		
		return str;
	}

}
