/**
 * 
 */
package edu.stanford.pigir.arcspread;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BinSedesTuple;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import edu.stanford.pigir.pigudf.PartOfSpeechTag;

/**
 * Facility to tag arbitrary text with part-of-speech tags. 
 * Special facilities are provided for HTML text.
 *
 * Provides part-of-speech tagging for HTML, and untagged text.
 * Relies on the Stanford Part-of-Speech-Tagger. Four initializers
 * are available to control the behavior of the tagger.
 * 
 * The tagging behavior of a POSTagger instance is set once and 
 * for all by how it is created. Subsequently, the <code>tag</code>
 * method may be called many times with different texts. 
 * Behaviors based on three parameters to the initializer methods:
 * <ul>
 *     <li>Only generate output for text within a given set of HTML/XML tags.
 *     <li>Only generate output for a small number of POS tags, as
 *     	   defined in 'Simplified Part of Speech Tags' below. The normally
 *     	   very fine grained variants of grammatical entities are mapped 
 *     	   into simple forms.
 *         For example: Noun (NN), and noun singular (NNS) are both tagged
 *         with Noun (NN) if this option is <code>true</code> 
 *     <li>Only generate output for an explicitly provided set of parts of speech tags.
 * </ul>
 * 
 * Full list of POS tags:
 * <ol>
 * <li>CC Coordinating conjunction
 * <li>CD Cardinal number
 * <li>DT Determiner
 * <li>EX Existential there
 * <li>FW Foreign word
 * <li>IN Preposition or subordinating conjunction
 * <li>JJ Adjective
 * <li>JJR Adjective, comparative
 * <li>JJS Adjective, superlative
 * <li>LS List item marker
 * <li>MD Modal
 * <li>NN Noun, singular or mass
 * <li>NNS Noun, plural
 * <li>NNP Proper noun, singular
 * <li>NNPS Proper noun, plural
 * <li>PDT Predeterminer
 * <li>POS Possessive ending
 * <li>PRP Personal pronoun
 * <li>PRP$ Possessive pronoun
 * <li>RB Adverb
 * <li>RBR Adverb, comparative
 * <li>RBS Adverb, superlative
 * <li>RP Particle
 * <li>SYM Symbol
 * <li>TO to
 * <li>UH Interjection
 * <li>VB Verb, base form
 * <li>VBD Verb, past tense
 * <li>VBG Verb, gerund or present participle
 * <li>VBN Verb, past participle
 * <li>VBP Verb, non�3rd person singular present
 * <li>VBZ Verb, 3rd person singular present
 * <li>WDT Wh�determiner
 * <li>WP Wh�pronoun
 * <li>WP$ Possessive wh�pronoun
 * <li>WRB Wh�adverb
 * </ol>
 * 
 * <b>Simplified Parts of Speech Tags</b>
 * 
 * <ul>
 *   <li>"SVN"    -->   "NN":   Noun</li>
 *   <li>"NNS"   -->   "NN":   Noun singluar</li>
 *   <li>"NNP"   -->   "NNP":  Proper noun</li>
 *   <li>"NNPS"  -->   "NNP":  Proper noun plural</li>
 *   <li>"RB"    -->   "RB":   Adverb</li>
 *   <li>"RBR"  	-->   "RB":   Adverb comparative</li>
 *   <li>"RBS"  	-->   "RB":   Adverb superlative</li>
 *   <li>"JJ"    -->   "JJ":   Adjective</li>
 *   <li>"JJR"  	-->   "JJ":   Adjective comparative</li>
 *   <li>"JJS"  	-->   "JJ":   Adjective superlative</li>
 *   <li>"IN"    -->   "IN":   Preposition (e.g. 'over')</li>
 *   <li>"PRP$"  -->   "PRP$": Possessive pronoun (e.g. 'my')</li>
 *   <li>"VB"    -->   "VB":   Verb base form</li>
 *   <li>"VBD"  	-->   "VB":   Verb past tense</li>
 *   <li>"VBG"  	-->   "VB":   Verb gerund or present participle</li>
 *   <li>"VBN"  	-->   "VB":   Verb past participle</li>
 *   <li>"VBP"  	-->   "VB":   Verb non-3rd person singular present</li>
 *   <li>"VBZ"  	-->   "VB":   Verb 3rd person singular present</li>
 * </ul>
 * @author paepcke
 */
public class POSTagger implements Iterator<List<String>> {
	
	final static boolean USE_SIMPLIFIED_POS_TAGS = true;
	final static boolean USE_FULL_POS_TAGS = false;
	
	PartOfSpeechTag tagger = null;
	List<Object> wordAndTagList = null;
	Iterator<Object> wordAndTagListIterator = null;
	String content;
	HashMap<String,Set<String>> wordToTagMap = null;

	/*---------------------------------
	 * Constructor
	 *--------------*/
	
	/**
	 * Obtain a tagger instance for your content.
	 * @param contentToTag: a String containing all of your content.
	 */
	public POSTagger(String contentToTag) {
		content = contentToTag;
	}

	
	// ------------------------------------------------------------   Public Methods -----------------------------------------	
		
	/*---------------------------------
	 * tag(String)
	 *--------------*/
	
	/**
	 * All text will be POS-tagged with the simplified POS tags.
	 * @return: An Iterable that feeds out word-POSTag pairs.
	 */
	public POSTagger tag() {
		// No filtering by HTML tags, simplified tag set, no POS tag filtering:
		return tagWorkhorse(content, null, USE_SIMPLIFIED_POS_TAGS, null);
		
	}
	
	/*---------------------------------
	 * tag(String)
	 *--------------*/

	/**
	 * Only text within provided HTML tags will be POS-tagged, with simplified POS tag set.
	 * 
     * @param htmlTags: Space-separated string of HTML tags. Only text within those
     *                  HTML tags in <code>content</code> will be POS-tagged. Example: "h1 p".
     *                  Pass <code>null</code> if inapplicable.
	 * @return: An Iterable that feeds out word-POSTag pairs.
	 */
	public POSTagger tag(String htmlTags) {
    	// Filtering by HTML tags, simplified tag set, no POS tag filtering:
		return tagWorkhorse(content, htmlTags, USE_SIMPLIFIED_POS_TAGS, null);
	}
	
	/*---------------------------------
	 * tag(String, boolean)
	 *--------------*/

	/**
	 * Only text within provided HTML tags will be POS-tagged. The second
	 * parameter controls whether only the simplified set of POS-tags should
	 * be used.
     * @param htmlTags: Space-separated string of HTML tags. Only text within those
     *                  HTML tags in this tagger's content will be POS-tagged. Example: "h1 p".
     *                  Pass <code>null</code> if inapplicable.
     * @param simplifiedPOSTags: Map very granular POS tags into a simplified set.
     *                  Pass <code>false</code> if full POS tag granularity is wanted.
	 * @return: An Iterable that feeds out word-POSTag pairs.
	 */

	public POSTagger tag(String htmlTags, boolean simplifiedPOSTags) {
		// Filter by HTML tags, simplified tag set dependent on in-parm, no POS tag filtering:
		return tagWorkhorse(content, htmlTags, simplifiedPOSTags, null);
	}
	
	/*---------------------------------
	 * tag(String, boolean, String)
	 *--------------*/

	/**
	 * Full control over all three behaviors.
	 * 
     * @param htmlTags: Space-separated string of HTML tags. Only text within those
     *                  HTML tags in <code>content</code> will be POS-tagged. Example: "h1 p".
     *                  Pass <code>null</code> if inapplicable.
     * @param simplifiedPOSTags: Map very granular POS tags into a simplified set.
     *                  Pass <code>false</code> if full POS tag granularity is wanted.
     * @param posTagList: Space-separated list of POS tags to which output is to be limited.
     *                  Pass <code>null</code> if inapplicable.  
	 * @return: An Iterable that feeds out word-POSTag pairs.
	 */
	
	public POSTagger tag(String htmlTags, boolean simplifiedPOSTags, String posTags) {
		// All aspects explicitly controlled:
		return tagWorkhorse(content, htmlTags, simplifiedPOSTags, posTags);
	}
	
	/*---------------------------------
	 * hasNext 
	 *--------------*/

	/* (non-Javadoc)
	 * @see java.util.Iterator#hasNext()
	 */
	public boolean hasNext() {
		return wordAndTagListIterator.hasNext();
	}

	/*---------------------------------
	 * next 
	 *--------------*/
	
	/* (non-Javadoc)
	 * @see java.util.Iterator#next()
	 * @return: Returns a list of two strings: the first
	 * is a word from the tagger's content, the second
	 * is the corresponding tag.
	 */
	public List<String> next() {
		BinSedesTuple nextWordTagTuple = (BinSedesTuple) wordAndTagListIterator.next();
		final List<Object> nextWordTagList = nextWordTagTuple.getAll();
		@SuppressWarnings("serial")
		ArrayList<String> retList = new ArrayList<String>() {
				{
					add((String) nextWordTagList.get(0));
					add((String) nextWordTagList.get(1));
				}
		};
		return retList; 
	}
	
	/*---------------------------------
	 * getAll() 
	 *--------------*/
	
	/**
	 * Obtain all of the result packaged in a 
	 * HashMap. This Map associates words with a Set of all
	 * POS tags that were assigned to that word anywhere
	 * in the content. Often this set will just have one
	 * POS tag member. But some words might have different
	 * tags in different contexts.
	 * @return: a HashMap<String, Set<String>> from a content
	 * word to a set with all the POS tags assigned to that
	 * word. 
	 */
	public HashMap<String, Set<String>> getAll() {
		// Even if wordToTagMap exists from a prior
		// call to getAll(), we destroy that one, and
		// start over, because a new tag() call might
		// have been made, changing the POS tags and 
		// words:
		wordToTagMap = new HashMap<String, Set<String>>();

		try {
			for (Object wordTagTuple : wordAndTagList) {
				String word   = (String) ((BinSedesTuple) wordTagTuple).get(0);
				String posTag = (String) ((BinSedesTuple) wordTagTuple).get(1);
				Set<String> allPOSTagsForWord = wordToTagMap.get(word); 
				if (allPOSTagsForWord == null) {
					allPOSTagsForWord = new HashSet<String>();
				}
				allPOSTagsForWord.add(posTag);
				wordToTagMap.put(word, allPOSTagsForWord);
			} 
		} catch (ExecException e) {
				// Debug if this ever happens:
				e.printStackTrace();
		}
		return wordToTagMap;
	}
	
	/*---------------------------------
	 * remove 
	 *--------------*/

	public void remove() {
		throw new RuntimeException("Cannot remove items from POS tag result.");
	}
	
	// ------------------------------------------------------------    Private Methods -----------------------------------------
	
	
	/*---------------------------------
	 * tagWorkhorse 
	 *--------------*/
	
	private POSTagger tagWorkhorse(String content, String htmlTags, boolean simplifiedPOSTags, String posTags) {
		
		PartOfSpeechTag tagger = new PartOfSpeechTag(htmlTags, (simplifiedPOSTags ? "true" : "false"), posTags);
		Tuple result = null;
		
		TupleFactory tupleFac = TupleFactory.getInstance();
		Tuple parms = tupleFac.newTuple(1);
		
		try {
			parms.set(0, content);
			result = tagger.exec(parms);
		} catch (ExecException e) {
			e.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		wordAndTagList = result.getAll();
		wordAndTagListIterator = wordAndTagList.iterator();
		return this;
	}

	
	// ------------------------------------------------------------    Main (Example)  -----------------------------------------
	
	/*---------------------------------
	 * main 
	 *--------------*/
	
	/**
	 * This module is intended for use with applications.
	 * Here is a runnable example.
	 * @param args: no command line arguments are expected.
	 */
	public static void main(String[] args) {
		String content = "<html><head></head><body><h1>Why I am so famous</h1>I will tell you why that is!</body></html>";
		
		// Get a tagger object for this content:
		POSTagger tagger = new POSTagger(content);

		// Request POS tagging only of text within 'h1' HTML tags:
		tagger.tag("h1");
		// The tagger is an Iterator that feeds out
		// lists of two strings: a word, and its tag.
		// Cycle through that iterator:
		while (tagger.hasNext()) {
			List<String> wordTagPair = (List<String>) tagger.next();
			System.out.println("Word: " + wordTagPair.get(0) + "; Tag: " + wordTagPair.get(1));
		}
		
		// Alternatively: get all results in a single HashMap. 
		// That map associates each word with a set of all POS
		// tags with which that word was tagged in your content.
		// Specifically, the Map is a HashMap<String,Set<String>>. 
		System.out.println("And now the same, using the getAll() method...");
		System.out.println("The square brackets indicate Set objects containing");
		System.out.println("all of the POS tags assigned to the corresponding word:");
		HashMap<String, Set<String>> wordToTagsMap = tagger.getAll();
		for (String word : wordToTagsMap.keySet()) {
			System.out.println("Word: " + word + "; Tags: " + wordToTagsMap.get(word));
		}
	}
}
