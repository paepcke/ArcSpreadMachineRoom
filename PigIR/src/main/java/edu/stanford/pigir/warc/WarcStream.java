/**
 * 
 */
package edu.stanford.pigir.warc;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;

import edu.stanford.pigir.webbase.Metadata;
import edu.stanford.pigir.webbase.WbTextRecord;

/**
 * @author paepcke
 *
 */
abstract public class WarcStream {
	
	protected File warcFileObj;
	protected int currentRecordIndex = -1;
	protected WarcRecordReader warcRecordReader = null;
	
	protected WarcStream(String theWarcFilePath) throws IOException {
		this(new File(theWarcFilePath));
	}
	
	protected WarcStream(File theWarcFileObj) throws IOException {
		warcFileObj = theWarcFileObj;
		warcRecordReader = new WarcRecordReader();
		warcRecordReader.initialize(warcFileObj);
	}
	
	protected boolean nextKeyValue() throws IOException {
		return warcRecordReader.nextKeyValue(WarcRecordReader.DO_READ_CONTENT);
	}
	
	protected WarcRecord getCurrentValue() {
		return warcRecordReader.getCurrentValue();
	}
	
	/**
	 * Given a WARC record instance, return an equivalent WbTextRecord
	 * instance.
	 * @param docID: Document ID to be passed into the resulting WbTextRecord.
	 * @param warcRecord: The WARC record instance to be converted.
	 * @return: A WbTextRecord that is usable in a WebStream.
	 */
	protected WbTextRecord makeWbRecord(int docID, WarcRecord warcRecord) {
		//String uuidStr = warcRecord.get(WarcRecord.WARC_RECORD_ID);
		int thePageSize = -1;
		try {
			thePageSize = Integer.parseInt(warcRecord.get(WarcRecord.CONTENT_LENGTH));
		} catch (Exception e) {}

		// Time stamp format: 20050904151535:
		String timeStamp = "YYYYMMDDhhmmss";
		try {
			timeStamp = warcRecord.get(WarcRecord.WARC_DATE);
		} catch (Exception e) {}
		
		String url = "";
		try {
			url = warcRecord.get(WarcRecord.WARC_TARGET_URI);
		} catch (Exception e) {};

		Metadata md = new Metadata(docID, thePageSize, currentRecordIndex, timeStamp, url);
		// Content comes as byte[]. Get a string:
		String contentStr = new String(warcRecord.getContentUTF8());
		
		String httpHeader = extractHTTPHeaderFromWARCContent(contentStr);
		
		WbTextRecord wbRecord = null;
		//public WbRecord(Metadata md, String httpHeader, byte[] content){
		try {
			wbRecord = new WbTextRecord(md, httpHeader, contentStr.getBytes("UTF-8"));
		} catch (UnsupportedEncodingException e) {}; // We *know* that UTF-8 is a known encoding.
		return wbRecord;
	}
	
	/**
	 * Take a Web page with HTTP header at its beginning. 
	 * Return just the HTTP header. Return may be empty string.
	 * @param httpHeaderPlusPage: String containing the HTTP header plus Web page.
	 * @return HTTP header as string.
	 */
	private String extractHTTPHeaderFromWARCContent(String httpHeaderPlusPage) {
		BufferedReader pageStrReader = null;
		try {
			InputStream is = new ByteArrayInputStream(httpHeaderPlusPage.getBytes("UTF-8"));
			InputStreamReader isr = new InputStreamReader(is);
			pageStrReader = new BufferedReader(isr);
		} catch (UnsupportedEncodingException e) {}; // We *know* that UTF-8 is a known encoding.
		String httpHeader = "";
		String headerLine = "";
		try {
			while ((headerLine = pageStrReader.readLine()) != null) {
				if (headerLine.isEmpty())
					return httpHeader;
				httpHeader += headerLine;
			}
		} catch (IOException e) {
			// Just return what we collected so far:
			return httpHeader;
		}
		// Ran out of Web page content before finding
		// end of HTTP header. Return what we found:
		return httpHeader;
	}
}