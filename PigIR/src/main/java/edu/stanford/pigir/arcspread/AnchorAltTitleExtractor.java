package edu.stanford.pigir.arcspread;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import edu.stanford.pigir.pigudf.AnchorAltTitleText;

 
/**
 * @author paepcke
 *
 * Facility to extract anchor texts, image ALT texts,
 * and image Title texts from HTML content.
 */
public class AnchorAltTitleExtractor {

	/*---------------------------------
	 * extract 
	 *--------------*/
	
	/**
	 * Given an HTML string, extract all link anchor texts, 
	 * and/or img tag 'ALT' attribute texts, and/or img tag 'title' 
	 * attribute texts.
	 * 
	 * @param content The HTML string to process
	 * @param getAnchor Whether or not link anchor text should be included in the result.
	 * @param getAlt Whether or not image Alt attribute text should be included in the result.
	 * @param getImgTitle Whether or not image Title attribute text should be included in the result.
	 * @return List of String with the requested information.
	 */
	public static List<Object> extract(String content, 
									   boolean getAnchor, 
									   boolean getAlt, 
									   boolean getImgTitle) {
		AnchorAltTitleText func = null;
		try {
			func = new AnchorAltTitleText((getAnchor    ? "true" : "false"),
										  (getAlt       ? "true" : "false"),
										  (getImgTitle  ? "true" : "false"));
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		TupleFactory tupleFac = TupleFactory.getInstance();
		Tuple parms = tupleFac.newTuple(1);

		Tuple result = null;
		try {
			parms.set(0, content);
			result = func.exec(parms);
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
		return result.getAll();
	}

	// ------------------------------------------------------------    Main (Example)  -----------------------------------------

	
	/**
	 * This class is intended to be used by applications.
	 * Nonetheless, here is an example.
	 * @param args No command line arguments are expected.
	 */
	public static void main(String[] args) {
		final boolean GET_ANCHOR_TEXT    = true;
		final boolean GET_ALT_TEXT       = true;
		final boolean GET_IMG_TITLE_TEXT = true;
		
		String content = "<html><head></head><body>This is <a href=\"foo/bar\">my link</a> and " +
						 "my picture <img source=\"/my/picture\" alt=\"my pix\">. So there!</body></html>";
		List<Object>resList = AnchorAltTitleExtractor.extract(content,
				                                              GET_ANCHOR_TEXT,
				                                              GET_ALT_TEXT,
				                                              GET_IMG_TITLE_TEXT);
		Iterator<Object> it = resList.iterator();
		while (it.hasNext()) {
			System.out.println((String) it.next());
		}
		
		// Or simply:
		System.out.println(resList.toString());

	}
}
