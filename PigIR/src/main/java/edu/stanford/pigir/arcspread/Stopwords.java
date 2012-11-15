/**
 * 
 */
package edu.stanford.pigir.arcspread;

import java.util.HashMap;
import java.util.Set;

import edu.stanford.pigir.pigudf.IsStopword;

/**
 * Provides stopword services. Those services are
 * all provided as static methods. <code>isStopword()</code>
 * tests whether a given word is a stopword. Method
 * <code>getStopwords()</code> returns a list of all
 * services. Method <code>setStopwords()</code> allows
 * callers to modify the stopword list. Methods for adding
 * and removing individual stopwords are provided as well. 
 * @author paepcke
 */
public class Stopwords extends IsStopword {

	/*---------------------------------
	 * isStopword 
	 *--------------*/

	/**
	 * Test whether a given word is a stopword.
	 * @param word the word to test.
	 * @return true if word is a stopword, else false;
	 */
	public static Boolean isStopword(String word) {
		return IsStopword.isStopword(word);
	}
	
	/**
	 * Obtains the currently used list of stopwords.
	 * @return a set containing all stopwords currently in use
	 * by method <code>isStopword()</code>. 
	 */
	public static Set<String> getStopwords() {
		return IsStopword.stopwords.keySet();
	}
	

	/*---------------------------------
	 * setStopwords 
	 *--------------*/
	
	/**
	 * Replace the set of stopwords.
	 * @param newStopwordSet a Set of the new stopwords.
	 */
	public static void setStopwords(Set<String> newStopwordSet) {
		HashMap<String, Boolean> newStopwordMap = new HashMap<String,Boolean>();
		for (String word : newStopwordSet) {
			newStopwordMap.put(word, true);
		}
		IsStopword.stopwords = newStopwordMap;
	}
	
	/*---------------------------------
	 * addStopword 
	 *--------------*/
	
	/**
	 * Add a new word to the stopword list.
	 * @param word word to be added.
	 */
	public static void addStopword(String word) {
		IsStopword.stopwords.put(word, true);
	}
	
	/*---------------------------------
	 * removeStopword 
	 *--------------*/
	
	/**
	 * Causes the given word to no longer be a stopword.
	 * @param word word to be deleted from the stopword list.
	 */
	public static void removeStopword(String word) {
		IsStopword.stopwords.remove(word);
	}

	// ------------------------------------------------------------    Main (Example)  -----------------------------------------
	
	/**
	 * This class is intended for use by application.
	 * But here is an example:
	 * @param argv no command line args expected.
	 */
	public static void main(String[] argv) {
		System.out.println(Stopwords.getStopwords().toString());
		System.out.println("Is 'Bottleneck' a stopword: " + Stopwords.isStopword("Bottleneck").toString());
		Stopwords.addStopword("Bottleneck");
		System.out.println("After adding: is 'Bottleneck' a stopword: " + Stopwords.isStopword("Bottleneck").toString());
		Stopwords.removeStopword("Bottleneck");
		System.out.println("After deleting: is 'Bottleneck' a stopword: " + Stopwords.isStopword("Bottleneck").toString());
	}
}
