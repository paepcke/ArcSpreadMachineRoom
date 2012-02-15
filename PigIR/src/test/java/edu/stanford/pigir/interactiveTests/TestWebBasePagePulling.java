package edu.stanford.pigir.interactiveTests;

import java.io.IOException;

import edu.stanford.pigir.webbase.DistributorContact;
import edu.stanford.pigir.webbase.WbRecord;
import edu.stanford.pigir.webbase.wbpull.webStream.BufferedWebStreamIterator;

class TestWebPagePulling {
	public void trial(String crawlName) throws IOException {
		DistributorContact wbContact = DistributorContact.getCrawlDistributorContact(crawlName, 2);
		BufferedWebStreamIterator it = new BufferedWebStreamIterator(wbContact);
		WbRecord page;
		while (it.hasNext()) {
			page = it.next();
			System.out.println(page.getContent());
			System.out.println("-------------------------------------");
		}
	}

	public static void main(String[] argv) throws IOException {
		new TestWebPagePulling().trial("09-2004");
		System.out.println("Done");
	}
}
