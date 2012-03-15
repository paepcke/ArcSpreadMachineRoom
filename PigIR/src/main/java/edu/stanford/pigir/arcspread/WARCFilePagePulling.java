package edu.stanford.pigir.arcspread;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import edu.stanford.pigir.warc.WarcRecord;
import edu.stanford.pigir.warc.WarcStreamIterator;
import edu.stanford.pigir.webbase.WbTextRecord;

/**
 * @author paepcke
 * Here is an example for how to pull Web pages out of a WARC file
 * on your local machine. Assume you've put your WARC file into your
 * Maven setup like this: <projectRoot>/src/main/resources/yourWARCFile.pages.gz.
 * Don't unzip the file. When you "mvn compile" your project, that file will
 * be copied to <projectRoor>/target/classes, which is why the ...getResource()...
 * below works.
 * 
 * The principle is that you ask for an iterator over your WARC file. That
 * iterator will feed you WbTextRecord instances. You can check out the WbTextRecord
 * class, and its super WbRecord for the fine methods you can call on these instances.
 */
public class WARCFilePagePulling {
	
	public void printWarcRecords() throws IOException {
		URL warcFileUrl = getClass().getResource("/HurricaneCoverage20050904-text.pages.gz");
		File warcFile = new File(warcFileUrl.getFile()); 
		WarcStreamIterator warcIterator = new WarcStreamIterator(warcFile);
		while (warcIterator.hasNext()) {
			WbTextRecord wbRecord = warcIterator.next();
			String pageContent = wbRecord.getContentUTF8();
			System.out.println(pageContent);
			
			// One gotcha: 
			// The simple toString() method on a WbTextRecord just prints the record's
			// header. So something like System.out.println(wbRecord) will just
			// print the page header.
			// Therefore, use the following instead for operations that call
			// toString() on a WbTextRecord implicitly, like the System.out.println(wbRecord) 
			// above: System.out.println(wbRecord.toString(WarcRecord.INCLUDE_CONTENT));
		}
	}
	
	public static void main(String[] args) throws IOException {
		WARCFilePagePulling puller = new WARCFilePagePulling();
		puller.printWarcRecords();
	}
}
