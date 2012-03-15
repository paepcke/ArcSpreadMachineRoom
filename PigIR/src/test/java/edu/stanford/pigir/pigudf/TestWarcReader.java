/**
 * 
 */
package edu.stanford.pigir.pigudf;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import edu.stanford.pigir.warc.WarcRecord;
import edu.stanford.pigir.warc.WarcRecordReader;
import edu.stanford.pigir.warc.WarcStreamIterator;
import edu.stanford.pigir.webbase.WbTextRecord;

/**
 * @author paepcke
 *
 */
public class TestWarcReader {
	
	WarcRecordReader warcRecordReader = null;
	
	// Content of first WARC record (see TODO below):
	static String firstRecordGold = "URL: null\n" +
			"Date: 2012-03-07T22:08:24+00:00\n" +
			"Length: 198\n" +
			"Position: 0\n" +
			"DocId: 0\n" +
			"software: webvac\n" +
			"isPartOf: WebBase\n" +
			"description: Stanford WebVac crawl\n" +
			"format: WARC file version 1.0\n" +
			"conformsTo: http://www.iso.org/iso/iso_catalogue/catalogue_tc/catalogue_detail.htm?csnumber=44717\n";
	
	public TestWarcReader() {
	}
	
	public void init(String warcFileResourcePath) throws IOException {
		URL warcFileUrl = getClass().getResource(warcFileResourcePath);
		File warcFile = new File(warcFileUrl.getFile()); 
		warcRecordReader = new WarcRecordReader();
		warcRecordReader.initialize(warcFile);
	}
	
	public boolean nextKeyValue() throws IOException {
		return warcRecordReader.nextKeyValue();
	}
	
	public WarcRecord getCurrentValue() {
		return warcRecordReader.getCurrentValue();
	}
	
	
	/**
	 * Print the WARC records from a reader that returns WARCRecord objects.
	 * The preferred method is to use the WarcStreamIterator instead.
	 * See method printTestWbRecords() below:
	 * @throws IOException
	 */
	public void printTestWarcRecords() throws IOException {
		WarcRecord record = null;
		while (nextKeyValue()) {
			record = getCurrentValue();
		if (record != null)
			System.out.println(record.toString(WarcRecord.INCLUDE_CONTENT));
		else
			System.out.println("Null record!");
		}
		System.out.println("Done");
	}
	
	/**
	 * Get a WarcStreamIterator, which feeds out WbTextRecord objects:
	 * @throws IOException
	 */
	public void printTestWbRecords() throws IOException {
		URL warcFileUrl = getClass().getResource("/HurricaneCoverage20050904-text.pages.gz");
		File warcFile = new File(warcFileUrl.getFile()); 
		WarcStreamIterator warcIterator = new WarcStreamIterator(warcFile);
		while (warcIterator.hasNext()) {
			WbTextRecord wbRecord = warcIterator.next();
			// The simple toString() method just prints the record
			// headers:
			System.out.println(wbRecord.toString(WarcRecord.INCLUDE_CONTENT));
		}
	}
	
	/**
	 * TODO: This method needs to be completed. It is supposed to 
	 * compare the first and last records of a WARC file against 
	 * known reference strings. 
	 * @throws IOException
	 */
	public static void compareFirstAndLast() throws IOException {
		WarcStreamIterator warcIterator = new WarcStreamIterator(
				new File("C:/Users/paepcke/dldev/EclipseWorkspaces/ArcSpreadMachineroom/PigIR/src/test/resources/HurricaneCoverage20050904-text.pages.gz"));
		WbTextRecord wbRecordFirst = null;
		WbTextRecord wbRecordLast = null;
		if (warcIterator.hasNext()) {
			wbRecordFirst = warcIterator.next();
		}
		while (warcIterator.hasNext()) {
			wbRecordLast = warcIterator.next();
		}
		System.out.println(wbRecordFirst.toString(WarcRecord.INCLUDE_CONTENT));
		System.out.println("----------------------");
		System.out.println(wbRecordLast.toString(WarcRecord.INCLUDE_CONTENT));
		
		int firstRecordGoldLen = firstRecordGold.length();
		
		String firstRecordHTTPHeader = new String(wbRecordFirst.getHTTPHeaderAsString().getBytes("UTF-8"));
		int firstRecordReadHTTPLen = firstRecordHTTPHeader.length();
		int firstRecordReadLen = wbRecordFirst.getContentUTF8().length(); 
		if (firstRecordReadHTTPLen + firstRecordReadLen != firstRecordGoldLen) {
			throw new RuntimeException("First WARC record and gold copy differ in length.");
		}
	}
	
	// ---------------------------------------- Main ---------------------------------------

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		TestWarcReader reader = new TestWarcReader();
		reader.init("/HurricaneCoverage20050904-text.pages.gz");		
		reader.printTestWarcRecords();
		System.out.println("--------------  Done Printing WARC Records  -----------");
		reader.printTestWbRecords();
		System.out.println("--------------  Done Printing WARC Records As WebBase Records -----------");
		//reader.compareFirstAndLast();
	}
}
