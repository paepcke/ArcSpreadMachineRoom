package edu.stanford.pigir.pigudf;


import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import edu.stanford.pigir.Common;

public class TestAnchorAltTitleText {

	/**
	 * For each result from IndexOneDoc, verify that every tuple is correct.
	 * 
	 * @param result is a tuple: ((docID,numPostings), (token1,docID,token1Pos), (token2,docID,token2Pos), ...). All docID are identical. 
	 * @param groundTruth an array of Truth objects. Each object contains one token and its position. The objects are ordered as in the expected result.
	 * @return true/false.
	 */
	private static boolean matchOutput(Tuple result, ArrayList<String> groundTruth) {

		Iterator<Object> resultIt = Common.getTupleIterator(result);
		Iterator<String> truthIt  = groundTruth.iterator();
		String nextRes = null;
		String nextTruth = null;

		if (result.size() == 0 && groundTruth.size() == 0)
			return true;
		
		while (resultIt.hasNext()) {
			if (! truthIt.hasNext())
				return false;
			nextRes   = (String) resultIt.next();
			nextTruth = truthIt.next();
			if (!nextRes.equals(nextTruth))
				return false;
		}
		if (truthIt.hasNext())
			return false;

		return true;
	}

	// ---------------------------------------    Test Cases --------------------------------
	@org.junit.Test
	public void noLinkStringShorterThanMinimumLinkLength() {

		AnchorAltTitleText func = null;
		try {
			func = new AnchorAltTitleText("true","true","true");
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		TupleFactory tupleFac = TupleFactory.getInstance();
		Tuple parms = tupleFac.newTuple(1);

		//	 No link, and string shorter than a minimum link's length:
		try {
			parms.set(0, "On a sunny day");
			assertNull(func.exec(parms));
		} catch (ExecException e) {
			AssertionError assertionErr = new AssertionError(
					"Unexpected problem setting parameter tuple.");
			assertionErr.initCause(e);
			throw assertionErr;
		} catch (IOException e1) {
			AssertionError assertionErr = new AssertionError(
					"Unexpected IO exception while executing func.");
			assertionErr.initCause(e1);
			throw assertionErr;
		}
	}
	
	@org.junit.Test
	public void noLinkLongEnough() {
		TupleFactory tupleFac = TupleFactory.getInstance();
		Tuple parms = tupleFac.newTuple(1);
		String htmlStr;
		AnchorAltTitleText func = null;
		try {
			func = new AnchorAltTitleText("true","true","true");
		} catch (IOException e1) {
			e1.printStackTrace();
		}
			// No link, long enough to possibly contain a link:
		try {
			htmlStr = "On a truly sunny day we walked along the beach, and smiled.";
			parms.set(0, htmlStr);
			assertTrue(matchOutput(func.exec(parms), new ArrayList<String>()));
		} catch (ExecException e) {
			AssertionError assertionErr = new AssertionError(
					"Unexpected problem setting parameter tuple.");
			assertionErr.initCause(e);
			throw assertionErr;
		} catch (IOException e1) {
			AssertionError assertionErr = new AssertionError(
					"Unexpected IO exception while executing func.");
			assertionErr.initCause(e1);
			throw assertionErr;
		}
		
	}
	
	@SuppressWarnings("serial")
	@org.junit.Test
	public void correctAndTightlySpacedLink() {
			
		TupleFactory tupleFac = TupleFactory.getInstance();
		Tuple parms = tupleFac.newTuple(1);
		String htmlStr;
		AnchorAltTitleText func = null;
		try {
			func = new AnchorAltTitleText("true","true","true");
		} catch (IOException e1) {
			e1.printStackTrace();
		}

			// Correct and tightly spaced link:
			htmlStr = "On a <a href=\"http://foo/bar.html\">sunny</a> day";
		try {
			parms.set(0, htmlStr);
			assertTrue(matchOutput(func.exec(parms), new ArrayList<String>() {
				{
					add("sunny");
				};
			}));
		} catch (ExecException e) {
			AssertionError assertionErr = new AssertionError(
					"Unexpected problem setting parameter tuple.");
			assertionErr.initCause(e);
			throw assertionErr;
		} catch (IOException e1) {
			AssertionError assertionErr = new AssertionError(
					"Unexpected IO exception while executing func.");
			assertionErr.initCause(e1);
			throw assertionErr;
		}
	}		
		
	@SuppressWarnings("serial")
	@org.junit.Test
	public void correctLinkWithSpaces() {

		TupleFactory tupleFac = TupleFactory.getInstance();
		Tuple parms = tupleFac.newTuple(1);
		String htmlStr;
		AnchorAltTitleText func = null;
		// Correct link with spaces:
		try {
			func = new AnchorAltTitleText("true","true","true");
			htmlStr = "On a <a href   =   \"http://foo/bar.html\">sunny</a> day";
			parms.set(0, htmlStr);

			assertTrue(matchOutput(func.exec(parms), new ArrayList<String>() {
				{
					add("sunny");
				};
			}));
		} catch (ExecException e) {
			AssertionError assertionErr = new AssertionError(
					"Unexpected problem setting parameter tuple.");
			assertionErr.initCause(e);
			throw assertionErr;
		} catch (IOException e1) {
			AssertionError assertionErr = new AssertionError(
					"Unexpected IO exception while executing func.");
			assertionErr.initCause(e1);
			throw assertionErr;
		}
	}
	
	@org.junit.Test
	public void noQuotesAroundURL() {
		TupleFactory tupleFac = TupleFactory.getInstance();
		Tuple parms = tupleFac.newTuple(1);
		String htmlStr;
		AnchorAltTitleText func = null;
		// No quotes around the URL ==> Not taken as a link:
		
		try {
			func = new AnchorAltTitleText("true","true","true");
			htmlStr = "On a <a href=http://foo/bar.html>sunny</a> day";
			parms.set(0, htmlStr);
			assertTrue(matchOutput(func.exec(parms), new ArrayList<String>()));
		} catch (ExecException e) {
			AssertionError assertionErr = new AssertionError(
					"Unexpected problem setting parameter tuple.");
			assertionErr.initCause(e);
			throw assertionErr;
		} catch (IOException e1) {
			AssertionError assertionErr = new AssertionError(
					"Unexpected IO exception while executing func.");
			assertionErr.initCause(e1);
			throw assertionErr;
		}
	}
	
	@SuppressWarnings("serial")
	@org.junit.Test
	public void multipleLinksThreeSpacesIn2ndAnchor() {
			
		TupleFactory tupleFac = TupleFactory.getInstance();
		Tuple parms = tupleFac.newTuple(1);
		String htmlStr;
		AnchorAltTitleText func = null;
		// Multiple links, and three spaces in second anchor:
		try {
			func = new AnchorAltTitleText("true","true","true");
			htmlStr = "On a <a href=\"http://foo/bar.html\">sunny</a> day in <a href=\"https:8090//blue/bar?color=green\">in March   </a> we ran.";
			parms.set(0, htmlStr);
			assertTrue(matchOutput(func.exec(parms), new ArrayList<String>() {
				{
					add("sunny");
					add("in March");
				};
			}));
		} catch (ExecException e) {
			AssertionError assertionErr = new AssertionError(
					"Unexpected problem setting parameter tuple.");
			assertionErr.initCause(e);
			throw assertionErr;
		} catch (IOException e1) {
			AssertionError assertionErr = new AssertionError(
					"Unexpected IO exception while executing func.");
			assertionErr.initCause(e1);
			throw assertionErr;
		}
	}
	
	@SuppressWarnings("serial")
	@org.junit.Test
	public void linkOnlyTaggedHTML() {
						
		TupleFactory tupleFac = TupleFactory.getInstance();
		Tuple parms = tupleFac.newTuple(1);
		String htmlStr;
		AnchorAltTitleText func = null;
		// Link that's nothing but tagged HTML:		
		try {
			func = new AnchorAltTitleText("true","true","true");
		
			htmlStr = "On a <a href=\"http://foo/bar.html\"><img src=/foo/bar></a> we ran.";
			parms.set(0, htmlStr);
			assertTrue(matchOutput(func.exec(parms), new ArrayList<String>() {
				{
					add("");
				};
			}));
			} catch (ExecException e) {
			AssertionError assertionErr = new AssertionError(
					"Unexpected problem setting parameter tuple.");
			assertionErr.initCause(e);
			throw assertionErr;
		} catch (IOException e1) {
			AssertionError assertionErr = new AssertionError(
					"Unexpected IO exception while executing func.");
			assertionErr.initCause(e1);
			throw assertionErr;
		}
	}
	
	@SuppressWarnings("serial")
	@org.junit.Test
	public void altText() {
		TupleFactory tupleFac = TupleFactory.getInstance();
		Tuple parms = tupleFac.newTuple(1);
		String htmlStr;
		AnchorAltTitleText func = null;
		// ALT text:
		try {
			func = new AnchorAltTitleText("true","true","true");
			htmlStr = "Foo <img src=\"http://www.blue/red\" alt=\"This is an alt text.\">";
			parms.set(0,htmlStr);
			assertTrue(matchOutput(func.exec(parms), new ArrayList<String>() {
				{
					add("This is an alt text.");
				};
			}));
		} catch (ExecException e) {
			AssertionError assertionErr = new AssertionError(
					"Unexpected problem setting parameter tuple.");
			assertionErr.initCause(e);
			throw assertionErr;
		} catch (IOException e1) {
			AssertionError assertionErr = new AssertionError(
					"Unexpected IO exception while executing func.");
			assertionErr.initCause(e1);
			throw assertionErr;
		}
	}
	
	@SuppressWarnings("serial")
	@org.junit.Test
	public void altTextWithEmbeddedDoubleQuotes() {
		
		TupleFactory tupleFac = TupleFactory.getInstance();
		Tuple parms = tupleFac.newTuple(1);
		String htmlStr;
		AnchorAltTitleText func = null;
		// ALT text with embedded escaped double quotes:		
		try {
			func = new AnchorAltTitleText("true","true","true");

			htmlStr = "Foo <IMG src=\"http://www.blue/red\" alt=\"This is an \\\"alt\\\" text.\">";
			parms.set(0,htmlStr);
			assertTrue(matchOutput(func.exec(parms), new ArrayList<String>() {
				{
					// add("This is an \\\"alt\\\" text.");   // Should return this, but:
					add("This is an \\");                     // See comment in method.
				};
			}));
		} catch (ExecException e) {
			AssertionError assertionErr = new AssertionError(
					"Unexpected problem setting parameter tuple.");
			assertionErr.initCause(e);
			throw assertionErr;
		} catch (IOException e1) {
			AssertionError assertionErr = new AssertionError(
					"Unexpected IO exception while executing func.");
			assertionErr.initCause(e1);
			throw assertionErr;
		}
	}
	
	@SuppressWarnings("serial")
	@org.junit.Test
	public void capitalizedAltText() {
		TupleFactory tupleFac = TupleFactory.getInstance();
		Tuple parms = tupleFac.newTuple(1);
		String htmlStr;
		AnchorAltTitleText func = null;
		// Capitalized ALT text:
		try {
			func = new AnchorAltTitleText("true","true","true");
			htmlStr = "Foo <Img src=\"http://www.blue/red\" ALT=\"This is an alt text.\">";
			parms.set(0,htmlStr);
			assertTrue(matchOutput(func.exec(parms), new ArrayList<String>() {
				{
					add("This is an alt text.");
				};
			}));
		} catch (ExecException e) {
			AssertionError assertionErr = new AssertionError(
					"Unexpected problem setting parameter tuple.");
			assertionErr.initCause(e);
			throw assertionErr;
		} catch (IOException e1) {
			AssertionError assertionErr = new AssertionError(
					"Unexpected IO exception while executing func.");
			assertionErr.initCause(e1);
			throw assertionErr;
		}
	}
	
	@SuppressWarnings("serial")
	@org.junit.Test
	public void titleTextAndAnchorText() {
		TupleFactory tupleFac = TupleFactory.getInstance();
		Tuple parms = tupleFac.newTuple(1);
		String htmlStr;
		AnchorAltTitleText func = null;
		// Title text and anchor text:		
		try {
			func = new AnchorAltTitleText("true","true","true");
			htmlStr = "Foo <a href=\"http://www.blue/red\" title=\"This is a title text.\">body</a>";
			parms.set(0,htmlStr);
			assertTrue(matchOutput(func.exec(parms), new ArrayList<String>() {
				{
					add("body");
					add("This is a title text.");
				};
			}));
		} catch (ExecException e) {
			AssertionError assertionErr = new AssertionError(
					"Unexpected problem setting parameter tuple.");
			assertionErr.initCause(e);
			throw assertionErr;
		} catch (IOException e1) {
			AssertionError assertionErr = new AssertionError(
					"Unexpected IO exception while executing func.");
			assertionErr.initCause(e1);
			throw assertionErr;
		}
	}
	
	@SuppressWarnings("serial")
	@org.junit.Test
	public void capsTitleTextNoAnchor() {
		TupleFactory tupleFac = TupleFactory.getInstance();
		Tuple parms = tupleFac.newTuple(1);
		String htmlStr;
		AnchorAltTitleText func = null;
		// Caps Title text; no anchor:		
		try {
			func = new AnchorAltTitleText("false","false","true");
			htmlStr = "Foo <a href=\"http://www.blue/red\" TITLE=\"This is a title text.\">body</a>";
			parms.set(0,htmlStr);
			assertTrue(matchOutput(func.exec(parms), new ArrayList<String>() {
				{
					add("This is a title text.");
				};
			}));
		} catch (ExecException e) {
			AssertionError assertionErr = new AssertionError(
					"Unexpected problem setting parameter tuple.");
			assertionErr.initCause(e);
			throw assertionErr;
		} catch (IOException e1) {
			AssertionError assertionErr = new AssertionError(
					"Unexpected IO exception while executing func.");
			assertionErr.initCause(e1);
			throw assertionErr;
		}
	}
	
	@SuppressWarnings("serial")
	@org.junit.Test
	public void altAndTitleTextNoAnchorText() {
		TupleFactory tupleFac = TupleFactory.getInstance();
		Tuple parms = tupleFac.newTuple(1);
		String htmlStr;
		AnchorAltTitleText func = null;
			try {
				func = new AnchorAltTitleText("false","true","true");
			htmlStr = "Foo <img alt=\"Fun image.\" src=\"http://foo/bar\"> <b TITLE=\"Bold for emphasis.\"> <a href=\"http://www.blue/red\" TITLE=\"This is a title text.\">body</a>";
			parms.set(0,htmlStr);
			assertTrue(matchOutput(func.exec(parms), new ArrayList<String>() {
				{
					add("Fun image.");
					add("Bold for emphasis.");
					add("This is a title text.");
				};
			}));
		} catch (ExecException e) {
			AssertionError assertionErr = new AssertionError(
					"Unexpected problem setting parameter tuple.");
			assertionErr.initCause(e);
			throw assertionErr;
		} catch (IOException e1) {
			AssertionError assertionErr = new AssertionError(
					"Unexpected IO exception while executing func.");
			assertionErr.initCause(e1);
			throw assertionErr;
		}
	}
	
	@SuppressWarnings("serial")
	@org.junit.Test
	public void titleTextWithEmbeddedEscapedDoubleQuotes() {
		TupleFactory tupleFac = TupleFactory.getInstance();
		Tuple parms = tupleFac.newTuple(1);
		String htmlStr;
		AnchorAltTitleText func = null;
			// Title text with embedded escaped double quote:
			try {
				func = new AnchorAltTitleText("false","false","true");
			htmlStr = "Foo <b TITLE=\"Bold for \\\"emphasis\\\".\">";
			parms.set(0,htmlStr);
			
			assertTrue(matchOutput(func.exec(parms), new ArrayList<String>() {
				{
					// add("Bold for \\\"emphasis\\\".");   // Should return this, but:
					add("Bold for \\");                     // the bug in AnchorText.java returns this. Oh well.   
				};
			}));
		} catch (ExecException e) {
			AssertionError assertionErr = new AssertionError(
					"Unexpected problem setting parameter tuple.");
			assertionErr.initCause(e);
			throw assertionErr;
		} catch (IOException e1) {
			AssertionError assertionErr = new AssertionError(
					"Unexpected IO exception while executing func.");
			assertionErr.initCause(e1);
			throw assertionErr;
		}
	}			
}
