package edu.stanford.pigir.pigudf;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class TestRest {
	

	@SuppressWarnings("serial")
	@org.junit.Test
	public void testInts() { 
		final TupleFactory tupleFac = TupleFactory.getInstance();
		Tuple tstTuple;
		Rest func = new Rest();
		
		try {
			tstTuple = tupleFac.newTuple(new ArrayList<Integer>() {
				{
					add(0, 0);
					add(1, 1);
					add(2, 2);
				};
			});
			Tuple res = tupleFac.newTuple();
			res.append(1);
			res.append(2);
			assertTrue(res.equals(func.exec(tstTuple)));
		} catch (IOException e1) {
			AssertionError assertionErr = new AssertionError(
					"Unexpected IO exception while executing func.");
			assertionErr.initCause(e1);
			throw assertionErr;
		}
	}	
			
	@SuppressWarnings("serial")
	@org.junit.Test
	public void singleString() { 
		final TupleFactory tupleFac = TupleFactory.getInstance();
		Tuple tstTuple;
		Rest func = new Rest();
		try {
			tstTuple = tupleFac.newTuple(new ArrayList<String>() {
				{
					add(0, "one");
				};
			});

			assertTrue(tupleFac.newTuple().equals(func.exec(tstTuple)));
			
			System.out.println("All tests passed.");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}

