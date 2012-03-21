package edu.stanford.pigir.pigudf;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class ExtractSingleHTMLTag extends EvalFunc<String> 
{
	public final Logger logger = Logger.getLogger(getClass().getName());
	
	//Tag types
	public static final String H1		= "h1";
	public static final String H2		= "h2";
	public static final String H3		= "h3";
	public static final String H4		= "h4";
	public static final String H5		= "h5";
	public static final String H6		= "h6";
	public static final String TITLE	= "title";
	
	//Tag patterns
	// Regex patterns to extract text. The '?' after the .* 
	// turns the  match non-greedy. Without 
	// the question mark, the .* would eat all the html.
	// We look for a not-escaped opening angle
	// bracket, followed by the tag name, etc.:
	public static final String H1_PATTERN		= "<h1(.*?)>(.*?)</h1>";
	public static final String H2_PATTERN		= "<h2(.*?)>(.*?)</h2>";
	public static final String H3_PATTERN		= "<h3(.*?)>(.*?)</h3>";
	public static final String H4_PATTERN		= "<h4(.*?)>(.*?)</h4>";
	public static final String H5_PATTERN		= "<h5(.*?)>(.*?)</h5>";
	public static final String H6_PATTERN		= "<h6(.*?)>(.*?)</h6>";
	public static final String TITLE_PATTERN	= "<title(.*?)>(.*?)</title>";
	
	//min html lengths
	public static final int HEADER_MIN_HTML_LENGTH	= "<h1></h1>".length();
	public static final int TITLE_MIN_HTML_LENGTH	= "<title></title>".length();
	
	//private local variables used to extract tag
	private String tagPattern;
	private String separator;
	private int minHtmlLength;
	
	public ExtractSingleHTMLTag(String tagType) throws IOException
	{
		this(tagType, " ");
	}
	
	public ExtractSingleHTMLTag(String tagType, String separator) throws IOException
	{
		//determine tag type
		if(tagType.equalsIgnoreCase(TITLE))
		{
			tagPattern 		= TITLE_PATTERN;
			minHtmlLength	= TITLE_MIN_HTML_LENGTH;
		}
		else if(tagType.equalsIgnoreCase(H1))
		{
			tagPattern 		= H1_PATTERN;
			minHtmlLength	= HEADER_MIN_HTML_LENGTH;
		}
		else if(tagType.equalsIgnoreCase(H2))
		{
			tagPattern 		= H2_PATTERN;
			minHtmlLength	= HEADER_MIN_HTML_LENGTH;
		}
		else if(tagType.equalsIgnoreCase(H3))
		{
			tagPattern 		= H3_PATTERN;
			minHtmlLength	= HEADER_MIN_HTML_LENGTH;
		}
		else if(tagType.equalsIgnoreCase(H4))
		{
			tagPattern 		= H4_PATTERN;
			minHtmlLength	= HEADER_MIN_HTML_LENGTH;
		}
		else if(tagType.equalsIgnoreCase(H5))
		{
			tagPattern 		= H5_PATTERN;
			minHtmlLength	= HEADER_MIN_HTML_LENGTH;
		}
		else if(tagType.equalsIgnoreCase(H6))
		{
			tagPattern 		= H6_PATTERN;
			minHtmlLength	= HEADER_MIN_HTML_LENGTH;
		}
		else
			throw new IOException("invalid or unimplemented tag type");
		
		this.separator = separator;
	}
	
	@Override
	public String exec(Tuple input) throws IOException 
	{
		String html = "";
		
		try
		{
			if((input.size() == 0) ||
					((html = (String) input.get(0)) == null) ||
					(html.length() < minHtmlLength))
			{
				return null;
			}
		}
		catch (ClassCastException e)
		{
			throw new IOException("ExtractSingleHTMLTag(): bad input: " + input);
		}
		
		return extractTag(html, this.separator);
	}
	
	public String extractTag(String html) throws IOException
	{
		return extractTag(html, this.separator);
	}	
	private String extractTag(String html, String separator) throws IOException
	{
		String output = "";				
		Pattern titleTextPattern = Pattern.compile(tagPattern, Pattern.CASE_INSENSITIVE);
		Matcher titleTextMatcher = titleTextPattern.matcher(html);
		while(titleTextMatcher.find())
		{
			output += StripHTML.extractText(titleTextMatcher.group());
			output += separator;
		}
		output = output.trim();
		
		return output;
	}

}
