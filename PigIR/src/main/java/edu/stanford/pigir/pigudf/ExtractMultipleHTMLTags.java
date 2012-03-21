package edu.stanford.pigir.pigudf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class ExtractMultipleHTMLTags extends EvalFunc<String> 
{
	private String separator	= " ";
	private String[] tags		= {};
	
	
	/**
	 * @param tags - a CSV string of the tags to extract
	 * @param separator (optional) - a string used to separate each element found 
	 */
	public ExtractMultipleHTMLTags(String tags, String separator)
	{
		this.separator	= separator;
		this.tags	 	= tags.split(",");
		
		for(String t : this.tags)
			t = t.trim();
	}
	public ExtractMultipleHTMLTags(String tags)
	{
		this(tags, " ");
	}
	
	@Override
	public String exec(Tuple input) throws IOException 
	{
		String output = "";
		String html = "";
		
		//get the html from the tuple
		try
		{
			if((input.size() == 0) ||
					((html = (String) input.get(0)) == null))
			{
				return null;
			}
		}
		catch (ClassCastException e)
		{
			throw new IOException("ExtractMultipleHTMLTags(): bad input: " + input);
		}
		
		//iterate through tags and append to output
		for(String t : this.tags)
		{
			try
			{
				ExtractSingleHTMLTag extractor = new ExtractSingleHTMLTag(t, this.separator);
				output += extractor.extractTag(html);
				output += this.separator;
			}
			catch(IOException e)
			{
				e.printStackTrace();
				continue;
			}
		}
		output = output.trim();
		
		return output;
	}

}
