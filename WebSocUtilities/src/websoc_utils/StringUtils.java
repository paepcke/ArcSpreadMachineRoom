package websoc_utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.regex.Pattern;


/***
 * This class contains functions to read files in different formats.
 * @author jyotika
 *
 */

public class StringUtils {

	public static Pattern TAB = Pattern.compile("\t");
	public static Pattern COMMA = Pattern.compile(",");
	public static Pattern NEWLINE = Pattern.compile("\n");
	public static Pattern SPACE = Pattern.compile(" ");
	public static Pattern COLONSPACE = Pattern.compile(": ");
	public static Pattern NONWORDS = Pattern.compile("[\\W]+");
	public static Pattern PUNCTUATIONS = Pattern.compile("[\\p{Punct}]");
	
	/***
	 * This functions reads a simple list file, where each item is on one line.
	 * @param fileName
	 * @return an ArrayList of String
	 * @throws Exception
	 */
	public static ArrayList<String> readListFile(String fileName) throws Exception{
		
		ArrayList<String> items = new ArrayList<String>();
		BufferedReader reader = new BufferedReader(new FileReader(fileName));
		
		String line = null;
		
		while (null!= (line = reader.readLine())) {
			items.add(line);
		}
		reader.close();
		
		return items;
	}
	
	/***
	 * This functions reads a file using the given reader, and returns the entire content as
	 * a String
	 * @param fileName
	 * @param reader
	 * @return String content
	 * @throws Exception
	 */
	public static String readEntire(String fileName, BufferedFileReader reader) throws Exception{
		
		StringBuffer content = new StringBuffer ();
	
		reader.open(fileName);
	
		String line = null;
	
		while (null != (line = reader.readLine())) {
			content.append(line);
			content.append(System.getProperty("line.seperator"));
		}
		
		reader.close();
		
		return content.toString();
	}
	
	/***
	 * Glob a file and return its content.
	 * @param fileName
	 * @return String content.
	 * @throws Exception
	 */
	
	public static String readEntire(String fileName) throws Exception{
		
		BufferedFileReader reader = new BufferedFileReader(fileName);
		StringBuffer content = new StringBuffer ();
	
		reader.open(fileName);
	
		String line = null;
	
		while (null != (line = reader.readLine())) {
			content.append(line);
			content.append("\n");//System.getProperty("line.seperator"));
		}
		
		reader.close();
		
		return content.toString();
	}
	
	
	/***
	 * This function reads a tab separated file with two fields and returns an
	 * ArrayList of Pairs.
	 * @param fileName
	 * @throws Exception
	 */
	public static ArrayList<Pair<String, String>> readTabSepFile
		(String fileName, boolean header) throws Exception {

		ArrayList<Pair<String, String>> pairs = new ArrayList<Pair<String,String>>();
		BufferedReader reader = new BufferedReader(new FileReader(fileName));
		
		String line;

		if (header)
			reader.readLine();
		while (null!= (line = reader.readLine())) {

			String[] parts = StringUtils.TAB.split(line);
			pairs.add(new Pair<String, String>(parts[0], parts[1]));
		}
		
		return pairs;

	}
	
}
