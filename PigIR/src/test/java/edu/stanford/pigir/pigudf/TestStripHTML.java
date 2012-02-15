package edu.stanford.pigir.pigudf;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;


public class TestStripHTML {

	@org.junit.Test
	public void normal() { 
		
		StripHTML func = new StripHTML();
		TupleFactory tupleFac = TupleFactory.getInstance();
		Tuple parms = tupleFac.newTuple(1);
		String htmlStr = "<head><html>This is <b>bold</b> and a <a href='http://test.com'>link anchor</a></html></head>";
		String groundTruth = "This is bold and a link anchor";
		
		try {
			// Something normal:
			parms.set(0, htmlStr);
			assertEquals(groundTruth, func.exec(parms));
		} catch (IOException e1) {
			AssertionError assertionErr = new AssertionError(
					"Unexpected IO exception while executing func.");
			assertionErr.initCause(e1);
			throw assertionErr;
		}
	}	
	
	@org.junit.Test
	public void upperCaseHref() { 
		StripHTML func = new StripHTML();
		TupleFactory tupleFac = TupleFactory.getInstance();
		Tuple parms = tupleFac.newTuple(1);
		String htmlStr;
		String groundTruth = "This is bold and a link anchor";
		
		try {
			// Upper case HREF:
			htmlStr = "<head><html>This is <b>bold</b> and a <a HREF='http://test.com'>link anchor</a></html></head>";
			parms.set(0, htmlStr);
			assertEquals(groundTruth, func.exec(parms));
		} catch (IOException e1) {
			AssertionError assertionErr = new AssertionError(
					"Unexpected IO exception while executing func.");
			assertionErr.initCause(e1);
			throw assertionErr;
		}
	}	
			
	@org.junit.Test
	public void emptyStr() { 
		StripHTML func = new StripHTML();
		TupleFactory tupleFac = TupleFactory.getInstance();
		Tuple parms = tupleFac.newTuple(1);
		try {
			// Empty string:
			parms.set(0,"");
			assertEquals("",func.exec(parms));
		} catch (IOException e1) {
			AssertionError assertionErr = new AssertionError(
					"Unexpected IO exception while executing func.");
			assertionErr.initCause(e1);
			throw assertionErr;
		}
	}	

	@org.junit.Test
	public void viaStaticMethod() { 
		String htmlStr = "<head><html>This is <b>bold</b> and a <a href='http://test.com'>link anchor</a></html></head>";
		String groundTruth = "This is bold and a link anchor";
		
		try {
			// Access to html stripping via the StripHTML class's static
			// extractText() method:
			assertEquals(groundTruth, StripHTML.extractText(htmlStr));
		} catch (IOException e1) {
			AssertionError assertionErr = new AssertionError(
					"Unexpected IO exception while executing func.");
			assertionErr.initCause(e1);
			throw assertionErr;
		}
	}	
}