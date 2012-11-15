package edu.stanford.pigir.arcspread;

import java.io.IOException;

import edu.stanford.pigir.webbase.DistributorContact;
import edu.stanford.pigir.webbase.WbRecord;
import edu.stanford.pigir.webbase.wbpull.webStream.BufferedWebStreamIterator;

/**
 * @author paepcke
 * Example of an application that pulls Web pages from
 * WebBase. If running outside of Eclipse, you can run 
 * the following command line arg in the PigIR root dir:
 *  mvn exec:java -Dexec.mainClass="edu.stanford.pigir.arcspread.WebBasePagePulling"
 */
class WebBasePagePulling {
	/**
	 * Method pulls 2 pages from the WebBase crawl called "09-2004".
	 * These crawl names can be found at:
	 * http://wb6.stanford.edu/~testbed/cgi-bin/crawlStreamingCrawlChoice.pl?numPages=1
	 * though this URL will change. Till then, don't push the buttons on 
	 * that page. 
	 * @param crawlName name of the crawl as obtained from the WebWase crawl directory.
	 * @throws IOException
	 */
	public void trial(String crawlName) throws IOException {
		// Obtain a 'distributor contact' object that will contain
		// all the information needed to access this particular crawl.
		// You include the total number of pages you want:
		//****DistributorContact wbContact = DistributorContact.getCrawlDistributorContact(crawlName, 2);
		DistributorContact wbContact = DistributorContact.getCrawlDistributorContact(
				crawlName, 
				100, 
				"www.usatoday.com",
				"www.usatoday.com");
		
		// Obtain an Iterator that will feed out one page at a time.
		// The startup might take a few moments. Complete failure can happen
		// if the machine that serves out this particular crawl is down:
		BufferedWebStreamIterator it = new BufferedWebStreamIterator(wbContact);
		
		// Pages are packaged in 'WbRecord' objects with a getContent() method:
		WbRecord page;
		while (it.hasNext()) {
			page = it.next();
			System.out.println(page.getContent());
			System.out.println("-------------------------------------");
		}
	}

	public static void main(String[] argv) throws IOException {
		//*****new WebBasePagePulling().trial("09-2004");
		new WebBasePagePulling().trial("uspapers-01-2010");
		System.out.println("Done");
	}
}
