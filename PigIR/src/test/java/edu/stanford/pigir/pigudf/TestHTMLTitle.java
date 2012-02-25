package edu.stanford.pigir.pigudf;

import static junit.framework.Assert.assertEquals;

import java.io.IOException;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import edu.stanford.pigir.pigudf.HTMLTitle;

public class TestHTMLTitle 
{
	public static void main(String[] args)
	{
		HTMLTitle		func		= new HTMLTitle();
		TupleFactory	tupleFac	= TupleFactory.getInstance();
		Tuple			parms		= tupleFac.newTuple(1);
		String			htmlStr		= "<head><title>This is the title.</title><html>This is <b>bold</b> and a <a href='http://test.com'>link anchor</a></html></head>";
		String			groundTruth	= "This is the title.";
		
		try 
		{
			//something normal
			parms.set(0, htmlStr);
			assertEquals(groundTruth, func.exec(parms));
			
			//Upper case TITLE
			htmlStr	= "<head><TITLE>This is the title.</TITLE><html>This is <b>bold</b> and a <a href='http://test.com'>link anchor</a></html></head>";
			assertEquals(groundTruth, func.exec(parms));
			
			//Empty String
			parms.set(0, "");
			assertEquals("", func.exec(parms));
		}
		catch(IOException e)
		{
			System.out.println("Failed with IOException: " + e.getMessage());
			System.exit(-1);
		}
		
		System.out.println("All tests passed.");
	}
}
