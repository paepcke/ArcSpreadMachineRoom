package edu.stanford.pigir.pigudf;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class HTMLTitle extends EvalFunc<String> 
{
	final static int MIN_LINK_LENGTH = "<title></title>".length();

	public final Logger logger = Logger.getLogger(getClass().getName());
	
	public HTMLTitle()
	{
		//...
	}
	
	@Override
	public String exec(Tuple input) throws IOException 
	{
		String html = null;
		String output = "";
		
		try
		{
			if((input.size() == 0) ||
					((html = (String) input.get(0)) == null) ||
					(html.length() < MIN_LINK_LENGTH))
			{
				return null;
			}
		}
		catch (ClassCastException e)
		{
			throw new IOException("HTMLTitle(): bad input: " + input);
		}
		
		// Matcher to extract anchor text. The '?' after the .* 
		// before the </title> turns this  match non-greedy. Without 
		// the question mark, the .* would eat all the html to 
		// the last </title>. We look for a not-escaped opening angle
		// bracket, followed by the title tag name, etc.:
		Pattern titleTextPattern = Pattern.compile("[^\\\\]<title>(.*?)</title>");
		Matcher titleTextMatcher = titleTextPattern.matcher(html);
		while(titleTextMatcher.find())
			output += StripHTML.extractText(titleTextMatcher.group());
		
		return output;
	}

}
