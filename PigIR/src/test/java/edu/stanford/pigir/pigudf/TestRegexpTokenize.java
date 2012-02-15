package edu.stanford.pigir.pigudf;

import java.io.IOException;
import static org.junit.Assert.*;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import edu.stanford.pigir.pigudf.RegexpTokenize;

public class TestRegexpTokenize {
	
		static RegexpTokenize func = new RegexpTokenize();
		static TupleFactory tupleFac = TupleFactory.getInstance();
		static Tuple parms = tupleFac.newTuple(1);
		static Tuple parmsTwo = tupleFac.newTuple(2);
		static Tuple parmsThree = tupleFac.newTuple(3);
		static Tuple parmsFour = tupleFac.newTuple(4);
		
		@org.junit.Test
		public void test1() { 
			try {

				parms.set(0, "On a sunny day");
				assertEquals("",func.exec(parms).toString(), "{(sunny),(day)}");
			} catch (IOException e1) {
				AssertionError assertionErr = new AssertionError(
						"Unexpected IO exception while executing func.");
				assertionErr.initCause(e1);
				throw assertionErr;
			}
		}	

	@org.junit.Test
	public void test2() {
		try {
			parms.set(0, "Testing it!");
			assertEquals("",func.exec(parms).toString(), "{(Testing)}");
		} catch (IOException e1) {
			AssertionError assertionErr = new AssertionError(
					"Unexpected IO exception while executing func.");
			assertionErr.initCause(e1);
			throw assertionErr;
		}
	}	
	
	@org.junit.Test
	public void test3() { 
		try {
			parms.set(0, "FDA");
			assertEquals("",func.exec(parms).toString(), "{(FDA)}");
		} catch (IOException e1) {
			AssertionError assertionErr = new AssertionError(
					"Unexpected IO exception while executing func.");
			assertionErr.initCause(e1);
			throw assertionErr;
		}
	}	

	@org.junit.Test
	public void test4() { 
		try {
			parmsTwo.set(1, "[\\s]");
			parmsTwo.set(0, "On a sunny day");
			assertEquals("",func.exec(parmsTwo).toString(), "{(sunny),(day)}");
		} catch (IOException e1) {
			AssertionError assertionErr = new AssertionError(
					"Unexpected IO exception while executing func.");
			assertionErr.initCause(e1);
			throw assertionErr;
		}
	}	
			
	@org.junit.Test
	public void test5() { 
		try {
			parmsTwo.set(0, "Testing it!");
			assertEquals("",func.exec(parmsTwo).toString(), "{(Testing),(it!)}");
		} catch (IOException e1) {
			AssertionError assertionErr = new AssertionError(
					"Unexpected IO exception while executing func.");
			assertionErr.initCause(e1);
			throw assertionErr;
		}
	}	

	@org.junit.Test
	public void test6() { 
		try {
			parmsTwo.set(0, "FDA");
			assertEquals("",func.exec(parmsTwo).toString(), "{(FDA)}");
		} catch (IOException e1) {
			AssertionError assertionErr = new AssertionError(
					"Unexpected IO exception while executing func.");
			assertionErr.initCause(e1);
			throw assertionErr;
		}
	}	

	@org.junit.Test
	public void test7() { 
		try {
			parmsThree.set(1, null);
			parmsThree.set(2, 1);
		} catch (IOException e1) {
			AssertionError assertionErr = new AssertionError(
					"Unexpected IO exception while executing func.");
			assertionErr.initCause(e1);
			throw assertionErr;
		}
	}	

	@org.junit.Test
	public void test8() { 
		try {
			parmsThree.set(0, "foo");
			assertEquals("",func.exec(parmsThree).toString(), "{(foo)}");
		} catch (IOException e1) {
			AssertionError assertionErr = new AssertionError(
					"Unexpected IO exception while executing func.");
			assertionErr.initCause(e1);
			throw assertionErr;
		}
	}	

	@org.junit.Test
	public void test9() { 
		try {
			parmsThree.set(0, "This is a stopword test.");
			assertEquals("",func.exec(parmsThree).toString(), "{(stopword),(test)}");
		} catch (IOException e1) {
			AssertionError assertionErr = new AssertionError(
					"Unexpected IO exception while executing func.");
			assertionErr.initCause(e1);
			throw assertionErr;
		}
	}
	
	@org.junit.Test
	public void test10() {
		try {
			parmsFour.set(1, null); // use standard regexp
			parmsFour.set(2, 0); // no stopword elimination
			parmsFour.set(3, 1);    // want URL preservation 
			
			parmsFour.set(0, "foo");
			assertEquals("",func.exec(parmsFour).toString(), "{(foo)}");
		} catch (IOException e1) {
			AssertionError assertionErr = new AssertionError(
					"Unexpected IO exception while executing func.");
			assertionErr.initCause(e1);
			throw assertionErr;
		}
	}
	
	@org.junit.Test
	public void test11() { 
		try {
			parmsFour.set(0, "http://infolab.stanford.edu");
			assertEquals("",func.exec(parmsFour).toString(), "{(http://infolab.stanford.edu)}");
		} catch (IOException e1) {
			AssertionError assertionErr = new AssertionError(
					"Unexpected IO exception while executing func.");
			assertionErr.initCause(e1);
			throw assertionErr;
		}
	}
	
	@org.junit.Test
	public void test12() { 
		try {
			parmsFour.set(0, "And now url (embedded http://infolab.stanford.edu) text");
			assertEquals("",func.exec(parmsFour).toString(), "{(And),(now),(url),(embedded),(http://infolab.stanford.edu),(text)}");
		} catch (IOException e1) {
			AssertionError assertionErr = new AssertionError(
					"Unexpected IO exception while executing func.");
			assertionErr.initCause(e1);
			throw assertionErr;
		}
	}	

	@org.junit.Test
	public void test13() { 
		try {
			parmsFour.set(0, "The word http text.");
			assertEquals("",func.exec(parmsFour).toString(), "{(The),(word),(http),(text)}");
		} catch (IOException e1) {
			AssertionError assertionErr = new AssertionError(
					"Unexpected IO exception while executing func.");
			assertionErr.initCause(e1);
			throw assertionErr;
		}
	}	

	@org.junit.Test
	public void test14() { 	
		try {
			parmsFour.set(0, "Finally, (file://C:/Users/kennedy/.baschrc) two URLs. ftp://blue.mountain.com/?parm1=foo&parm2=bar");
			assertEquals("",func.exec(parmsFour).toString(), "{(Finally),(file://C:/Users/kennedy/.baschrc),(two),(URLs),(ftp://blue.mountain.com/?parm1=foo&parm2=bar)}");
		} catch (IOException e1) {
			AssertionError assertionErr = new AssertionError(
					"Unexpected IO exception while executing func.");
			assertionErr.initCause(e1);
			throw assertionErr;
		}
	}	
	
	@org.junit.Test
	public void test15 () { 
		try {
			parmsTwo.set(1, "fo.*o");
		} catch (IOException e1) {
			AssertionError assertionErr = new AssertionError(
					"Unexpected IO exception while executing func.");
			assertionErr.initCause(e1);
			throw assertionErr;
		}
	}	

	@org.junit.Test
	public void test16() { 
		try {
			parmsTwo.set(0, "foo");
			assertEquals("",func.exec(parmsTwo).toString(), "{}");
		} catch (IOException e1) {
			AssertionError assertionErr = new AssertionError(
					"Unexpected IO exception while executing func.");
			assertionErr.initCause(e1);
			throw assertionErr;
		}
	}	

	@org.junit.Test
	public void test17() { 
		try {
			parmsTwo.set(0, "fobaro");
			assertEquals("",func.exec(parmsTwo).toString(), "{}");
		} catch (IOException e1) {
			AssertionError assertionErr = new AssertionError(
					"Unexpected IO exception while executing func.");
			assertionErr.initCause(e1);
			throw assertionErr;
		}
	}	
			
	@org.junit.Test
	public void test18() { 
		try {
			parmsTwo.set(0, "fobarotree");
			assertEquals("",func.exec(parmsTwo).toString(), "{(tree)}");
		} catch (IOException e1) {
			AssertionError assertionErr = new AssertionError(
					"Unexpected IO exception while executing func.");
			assertionErr.initCause(e1);
			throw assertionErr;
		}
	}	

	@org.junit.Test
	public void test19() { 
		try {
			parmsTwo.set(0, "fo is your papa barotree");
			assertEquals("",func.exec(parmsTwo).toString(), "{(tree)}");
		} catch (IOException e1) {
			AssertionError assertionErr = new AssertionError(
					"Unexpected IO exception while executing func.");
			assertionErr.initCause(e1);
			throw assertionErr;
		}
	}	

	@org.junit.Test
	public void test20() { 
		try {
			parmsTwo.set(0, "fo is your papa barotree and with you.");
			assertEquals("",func.exec(parmsTwo).toString(), "{(u.)}");
		} catch (IOException e1) {
			AssertionError assertionErr = new AssertionError(
					"Unexpected IO exception while executing func.");
			assertionErr.initCause(e1);
			throw assertionErr;
		}
	}	

	@org.junit.Test
	public void test21() { 
		assertEquals(RegexpTokenize.findURL("This is http://foo.bar.com/blue.html", 8), "http://foo.bar.com/blue.html");
	}	
			
			
	@org.junit.Test
	public void test22() { 
		assertEquals(RegexpTokenize.findURL("file://me.you.her/blue.html", 0), "file://me.you.her/blue.html");
	}	
			
	@org.junit.Test
	public void test23() { 
		assertEquals(RegexpTokenize.findURL("URL is ftp://me.you.her/blue.html, and embedded.", 7), "ftp://me.you.her/blue.html");
	}	

	@org.junit.Test
	public void test24() { 
		assertEquals(RegexpTokenize.findURL("No index given ftp://me.you.her/blue.html, and embedded."), "ftp://me.you.her/blue.html");
	}	

	@org.junit.Test
	public void test25() { 
		assertEquals(RegexpTokenize.findURL("file://me.you.her/blue.html without index"), "file://me.you.her/blue.html"); 
	}	
}
