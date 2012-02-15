package edu.stanford.pigir.pigudf;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import edu.stanford.pigir.Common;
import edu.stanford.pigir.pigudf.GetUUID;
import edu.stanford.pigir.pigudf.IndexOneDoc;

public class TestIndexOneDoc {
	
	static String uuid = null;

	class Truth {
		public String word;
		public int pos;

		public Truth(String theWord, int thePos) {
			word = theWord;
			pos = thePos;
		}

		public String toString() {
			return "Truth[(" + word + "," + pos + ")]";
		}
	}

	public TestIndexOneDoc() {
		if (TestIndexOneDoc.uuid == null) 
			TestIndexOneDoc.uuid  = GetUUID.newUUID();
	}

	/**
	 * For each result from IndexOneDoc, verify that every tuple is correct.
	 * 
	 * @param result is a tuple: ((docID,numPostings), (token1,docID,token1Pos), (token2,docID,token2Pos), ...). All docID are identical. 
	 * @param groundTruth an array of Truth objects. Each object contains one token and its position. The objects are ordered as in the expected result.
	 * @return true/false.
	 */
	private static boolean matchOutput(Tuple result, ArrayList<Truth> groundTruth) {

		Iterator<Object> resultIt = Common.getTupleIterator(result);
		Iterator<Truth> truthIt  = groundTruth.iterator();
		Tuple nextRes = null;
		Truth nextTruth = null;

		try {

			if (result.size() == 0 && groundTruth.size() == 0)
				return true;

			// Get the result summary: (docid,numPostings):
			//nextRes = (Tuple) resultIt.next();
			//summaryDocID = (String) nextRes.get(0);
			//summaryNumPostings = (Integer) nextRes.get(1);
			//if ((summaryDocID != uuid) || summaryNumPostings != groundTruth.size())
			//	return false;

			while (resultIt.hasNext()) {
				if (! truthIt.hasNext())
					return false;
				nextRes   = (Tuple) resultIt.next();
				nextTruth = truthIt.next();
				if (!nextRes.get(0).equals(nextTruth.word) || 
					!nextRes.get(1).equals(TestIndexOneDoc.uuid) || 
					!nextRes.get(2).equals(nextTruth.pos))
					return false;
			}
			if (truthIt.hasNext())
				return false;

			return true;
		} catch (ExecException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return true;
	}

	// ------------------------------------------------------  Test Cases ----------------------------

	@SuppressWarnings("serial")
	@org.junit.Test
	public void simple() {

		IndexOneDoc func = new IndexOneDoc();

		TupleFactory tupleFac = TupleFactory.getInstance();
		Tuple parms = tupleFac.newTuple(2);
		try {
			parms.set(0, TestIndexOneDoc.uuid);
		} catch (IOException e1) {
			AssertionError assertionErr = new AssertionError(
					"Unexpected IO exception while executing func.");
			assertionErr.initCause(e1);
			throw assertionErr;
		}

		final TestIndexOneDoc tester = new TestIndexOneDoc();

		try {
			// System.out.println(func.outputSchema(new Schema()));
			// Simple, straight forward:

			parms.set(1, "On a sunny day");
			assertTrue(matchOutput(func.exec(parms), new ArrayList<Truth>() {
				{
					add(tester.new Truth("na", 2));
					add(tester.new Truth("sunny", 2));
					add(tester.new Truth("day", 3));
				};
			})); 
		} catch (IOException e1) {
			AssertionError assertionErr = new AssertionError(
					"Unexpected IO exception while executing func.");
			assertionErr.initCause(e1);
			throw assertionErr;
		}
	}

	@SuppressWarnings("serial")
	@org.junit.Test
	public void embeddedURL() { 

		IndexOneDoc func = new IndexOneDoc();
		final int contentIndex = 1;

		TupleFactory tupleFac = TupleFactory.getInstance();
		Tuple parms = tupleFac.newTuple(2);
		try {
			parms.set(0, TestIndexOneDoc.uuid);
		} catch (IOException e1) {
			AssertionError assertionErr = new AssertionError(
					"Unexpected IO exception while executing func.");
			assertionErr.initCause(e1);
			throw assertionErr;
		}
		final TestIndexOneDoc tester = new TestIndexOneDoc();

		try {
			// Embedded URL:
			parms.set(contentIndex, "On a http://infolab.stanford.edu/~user sunny day.");
			assertTrue(matchOutput(func.exec(parms), new ArrayList<Truth>() {
				{
					add(tester.new Truth("na", 3));
					add(tester.new Truth("http://infolab.stanford.edu/~user", 2));
					add(tester.new Truth("sunny", 3));
					add(tester.new Truth("day", 4));
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
	public void justAURL() {
		IndexOneDoc func = new IndexOneDoc();
		final int contentIndex = 1;

		TupleFactory tupleFac = TupleFactory.getInstance();
		Tuple parms = tupleFac.newTuple(2);
		try {
			parms.set(0, TestIndexOneDoc.uuid);
		} catch (IOException e1) {
			AssertionError assertionErr = new AssertionError(
					"Unexpected IO exception while executing func.");
			assertionErr.initCause(e1);
			throw assertionErr;
		}

		final TestIndexOneDoc tester = new TestIndexOneDoc();

		try {
			// Just a URL:
			parms.set(contentIndex, "ftps://my.domain/");
			assertTrue(matchOutput(func.exec(parms), new ArrayList<Truth>() {
				{
					add(tester.new Truth("na", 1));
					add(tester.new Truth("ftps://my.domain/", 0));
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
	public void emptyString() {
		IndexOneDoc func = new IndexOneDoc();
		final int contentIndex = 1;

		TupleFactory tupleFac = TupleFactory.getInstance();
		Tuple parms = tupleFac.newTuple(2);
		try {
			parms.set(0, TestIndexOneDoc.uuid);
		} catch (IOException e1) {
			AssertionError assertionErr = new AssertionError(
					"Unexpected IO exception while executing func.");
			assertionErr.initCause(e1);
			throw assertionErr;
		}

		final TestIndexOneDoc tester = new TestIndexOneDoc();

		try {
			// Empty string:
			parms.set(contentIndex, "");
			assertTrue(matchOutput(func.exec(parms), new ArrayList<Truth>() {
				{
					add(tester.new Truth("na", 0));
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
	public void includeStopWords() { 

		IndexOneDoc func = new IndexOneDoc();

		TupleFactory tupleFac = TupleFactory.getInstance();
		Tuple parms = tupleFac.newTuple(2);
		try {
			parms.set(0, TestIndexOneDoc.uuid);
		} catch (IOException e1) {
			AssertionError assertionErr = new AssertionError(
					"Unexpected IO exception while executing func.");
			assertionErr.initCause(e1);
			throw assertionErr;
		}

		final TestIndexOneDoc tester = new TestIndexOneDoc();

		// Include stopwords:
		Tuple input = tupleFac.newTuple();
		input.append(TestIndexOneDoc.uuid);
		input.append("The sun is shining.");
		input.append(0); // no stopword elimination
		try {
			assertTrue(matchOutput(func.exec(input), new ArrayList<Truth>() {
				{
					add(tester.new Truth("na", 4));
					add(tester.new Truth("The", 0));
					add(tester.new Truth("sun", 1));
					add(tester.new Truth("is", 2));
					add(tester.new Truth("shining", 3));
				};
			}));
		} catch (IOException e1) {
			AssertionError assertionErr = new AssertionError(
					"Unexpected IO exception while executing func.");
			assertionErr.initCause(e1);
			throw assertionErr;
		}
	}

	@SuppressWarnings("serial")
	@org.junit.Test
	public void dontIgnoreStopwordsAndUseSpaceDelim() { 

		IndexOneDoc func = new IndexOneDoc();

		TupleFactory tupleFac = TupleFactory.getInstance();
		Tuple parms = tupleFac.newTuple(2);
		try {
			parms.set(0, TestIndexOneDoc.uuid);
		} catch (IOException e1) {
			AssertionError assertionErr = new AssertionError(
					"Unexpected IO exception while executing func.");
			assertionErr.initCause(e1);
			throw assertionErr;
		}

		final TestIndexOneDoc tester = new TestIndexOneDoc();
		Tuple input = tupleFac.newTuple();

		// Don't ignore stopwords and use SPACE as token delimiter:
		input = tupleFac.newTuple();
		input.append(TestIndexOneDoc.uuid);
		input.append("The sun is shining.");
		input.append(0); // no stopwords
		input.append(null); // default URL preservation
		input.append(" "); // SPACE as delimiter

		try {
			assertTrue(matchOutput(func.exec(input), new ArrayList<Truth>() {
				{
					add(tester.new Truth("na", 4));
					add(tester.new Truth("The", 0));
					add(tester.new Truth("sun", 1));
					add(tester.new Truth("is", 2));
					add(tester.new Truth("shining.", 3));
				};
			}));
		} catch (IOException e1) {
			AssertionError assertionErr = new AssertionError(
					"Unexpected IO exception while executing func.");
			assertionErr.initCause(e1);
			throw assertionErr;
		}
	}
}
