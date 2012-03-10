package edu.stanford.pigir.pigudf;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class TestExtractSingleHTMLTag 
{
	private static Tuple getHTMLTuple(String html)
	{
		TupleFactory tupleFac = TupleFactory.getInstance();
		Tuple t = tupleFac.newTuple(1);
		try 
		{
			t.set(0, html);
		} 
		catch (ExecException e) 
		{
			AssertionError ae = new AssertionError("Unexpected problem setting parameter tuple.");
			ae.initCause(e);
			throw ae;
		}
		
		return t;
	}
	
	@org.junit.Test
	public void testEmptyString()
	{
		Tuple empty = getHTMLTuple("");
		try 
		{
			//the function should return null if the html isn't long enough
			ExtractSingleHTMLTag func = new ExtractSingleHTMLTag("title");
			assertEquals(null, func.exec(empty));
			
			func = new ExtractSingleHTMLTag("h1");
			assertEquals(null, func.exec(empty));
			func = new ExtractSingleHTMLTag("h2");
			assertEquals(null, func.exec(empty));
			func = new ExtractSingleHTMLTag("h3");
			assertEquals(null, func.exec(empty));
			func = new ExtractSingleHTMLTag("h4");
			assertEquals(null, func.exec(empty));
			func = new ExtractSingleHTMLTag("h5");
			assertEquals(null, func.exec(empty));
			func = new ExtractSingleHTMLTag("h6");
			assertEquals(null, func.exec(empty));
		} 
		catch (IOException e) 
		{
			AssertionError ae = new AssertionError("Unexpected problem initializing function");
			ae.initCause(e);
			throw ae;
		}
	}
	
	@org.junit.Test
	public void testTitle()
	{
		Tuple	htmlTuple	= getHTMLTuple("<head><title>This is the title.</title><html>This is <b>bold</b> and a <a href='http://test.com'>link anchor</a></html></head>");
		String	groundTruth	= "This is the title.";
		
		try 
		{
			//something normal
			ExtractSingleHTMLTag func = new ExtractSingleHTMLTag("title");
			assertEquals(groundTruth, func.exec(htmlTuple));

			//upper case title
			htmlTuple.set(0, "<head><TITLE>This is the title.</TITLE><html>This is <b>bold</b> and a <a href='http://test.com'>link anchor</a></html></head>");
			assertEquals(groundTruth, func.exec(htmlTuple));
			
			//normal with an attribute
			htmlTuple.set(0, "<head><title lang=foo>This is the title.</title><html>This is <b>bold</b> and a <a href='http://test.com'>link anchor</a></html></head>");
			assertEquals(groundTruth, func.exec(htmlTuple));
			
			//upper case with attribute
			htmlTuple.set(0, "<head><TITLE lang=foo>This is the title.</TITLE><html>This is <b>bold</b> and a <a href='http://test.com'>link anchor</a></html></head>");
			assertEquals(groundTruth, func.exec(htmlTuple));
		} 
		catch (IOException e) 
		{
			AssertionError ae = new AssertionError("Unexpected problem initializing function");
			ae.initCause(e);
			throw ae;
		}
	}
	
	@org.junit.Test
	public void testHeaderTags()
	{
		//TODO ...
	}
}
