/**
 * 
 */
package edu.stanford.pigir.warc;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.lang.NotImplementedException;

import edu.stanford.pigir.webbase.WbRecord;
import edu.stanford.pigir.webbase.WbTextRecord;

/**
 * @author paepcke
 *
 */
public class WarcStreamIterator extends WarcStream 
	implements Iterator<WbRecord> {

	Boolean haveStagedWarcRecord = false;
	// Remember if nextKeyValue() returned encountered
	// an IOError while reading from the WARC file:
	IOException hadIOError = null;
	
	public WarcStreamIterator(File warcFileObj) throws IOException {
		super(warcFileObj);
		// Stage the first WARC record:
		haveStagedWarcRecord = nextKeyValue();
	}
	
	public boolean hasNext() {
		return haveStagedWarcRecord;
	}
	public WbTextRecord next(){
		if (hadIOError != null) {
			throw new RuntimeException("Error reading from WARC file: " + hadIOError.getMessage());
		}
		WarcRecord nextWarcRecord = getCurrentValue();
		if (nextWarcRecord == null) {
			return null;
		}
		currentRecordIndex++;
		WbTextRecord wbRecordRes = makeWbRecord(currentRecordIndex, nextWarcRecord);
		// Pre-fetch next WARC record from file, so that hasNext() will
		// know what to say:
		try {
			haveStagedWarcRecord = nextKeyValue();
		} catch (IOException e) {
			// If we have a problem pulling from the WARC file,
			// cause the next hasNext() method to return false:
			haveStagedWarcRecord = false;
			hadIOError = e;
		}
		return wbRecordRes;
	}
	
	public void remove() {
		throw new NotImplementedException("Cannot remove records from a WARC collection.");
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
}
