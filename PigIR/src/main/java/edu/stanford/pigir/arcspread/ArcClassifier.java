package edu.stanford.pigir.arcspread;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URL;

import edu.stanford.nlp.classify.Classifier;
import edu.stanford.nlp.classify.ColumnDataClassifier;
import edu.stanford.nlp.classify.GeneralDataset;
import edu.stanford.nlp.ling.Datum;
import edu.stanford.nlp.objectbank.ObjectBank;
import edu.stanford.nlp.util.ErasureUtils;

/**
 * Wrapper around Stanford classifier. Simple workflow:
 * <ul>
 * 		<li>Turn in text to no-tab, no-newline string.<br>
 * 			<code>ArcClassifier.textFilePrep(inFile, outFile)</code></li>
 *      <li>Create an instance of ArcClassifier, and either (a) train it, or
 *      	(b) have it load a classifier your trained before:
 *      	<code>
 *      		  myClassifier = new ArcClassifier(propFilePath);<br>
 *         (a)	  myClassifier.train(trainingFile);<br>  
 *         (b)    myClassifier.loadClassifier(classifierFile);<br>
 *          </code></li>
 *      <li>Optional, but recommended: test the classifier with a training set:
 *      	<code>myClassifier.test(testFile);</code></li>
 *      <li>Classify new data:<br>
 *      	<code>myClassifier.classify(inFile, outFile)</code></li>
 *      <li>Optionally save your classifier:<br>
 *      	<code>myClassifier.saveClassifier(outFile)</code></li>
 * </ul>
 * This module is usually imported, and used programmatically, not run
 * on the command line. The main() method is for testing.
 */

public class ArcClassifier extends ColumnDataClassifier {
	
	Classifier<String,String> trainedClassifier = null; 
	
	/**
	 * The properties file are instructions for the classifier. For this
	 * constructor the path specification is a Maven resource path. You place
	 * the property file into <projectRoot>/src/main/resources. A file myProps.prop
	 * would be specified as "myProps.prop" in this constructor.
	 * @see <url>http://www-nlp.stanford.edu/wiki/Software/Classifier</url>
	 * @param propertiesFileResourcePath Maven resource path for the property file.
	 * 		  This path is relative to <projectRoot>/target/classes. That is where
	 * 		  the compilation process copies files from <projectRoot>/src/main/resources.
	 */
	public ArcClassifier(String propertiesFileResourcePath) {
		super(ArcClassifier.getFilePath(propertiesFileResourcePath));
	}
	
	/**
	 * Constructor taking an absolute path to the property file. For property file information,
	 * @see <a href="http://www-nlp.stanford.edu/wiki/Software/Classifier">http://www-nlp.stanford.edu/wiki/Software/Classifier</a>
	 * @param propertiesFile File object containing the full path to the property file.
	 */
	public ArcClassifier(File propertiesFile) {
		super(propertiesFile.getAbsolutePath());
	}
	
	/**
	 * Turn a normal text file into a string of tokens without tabs. This format is 
	 * required for inputs to the trained classifier.
	 * @param inFilePath absolute path to the text file.
	 * @param outFilePath absolute path to the new file's destination.
	 * @throws IOException
	 */
	public static void textFilePrep(String inFilePath, String outFilePath) throws IOException {
		FileInputStream inStream = new FileInputStream(inFilePath);
		InputStreamReader inReader = new InputStreamReader(inStream);
		BufferedReader reader = new BufferedReader(inReader);
		FileOutputStream outStream = new FileOutputStream(outFilePath);
		
		String line;
		try {
			while ((line=reader.readLine()) != null) {
				String noTabsLine = line.replace("\t", " ");
				outStream.write(noTabsLine.getBytes("UTF-8"));
			}
		} catch (IOException e) {
			throw new IOException("During read/write of data file: " + e.getMessage());
		} finally {
			inReader.close();
			inStream.close();
			outStream.close();
		}
	}
	
	/**
	 * Train the classifier using a training file. For details on the training file format
	 * see the example cheese2007.train in this project's resources directory. The path 
	 * for this method's parameter is a Maven resource path. See header comment to one of
	 * the initializers for details.
	 * @param trainingSetResourcePath resource path to the training file. Example: "myFile.train" 
	 * 			for files in your project's <projectRoot>/src/main/resources directory. 
	 * @throws IOException
	 */
	public void train(String trainingSetResourcePath) throws IOException {
		String trainFilePath = ArcClassifier.getFilePath(trainingSetResourcePath);
		train(new File(trainFilePath));
	}
	
	/**
	 * Train the classifier using a training file. For details on the training file format
	 * see the example cheese2007.train in this project's resources directory. The parameter 
	 * for this method is an absolute path.
	 * @param trainingSetFile trainingSetResourcePath resource path to the training file. Example: "myFile.train" 
	 * 			for files in your project's <projectRoot>/src/main/resources directory.
	 * @throws IOException
	 */
	public void train(File trainingSetFile) throws IOException {
		GeneralDataset<String,String> genSet = readTrainingExamples(trainingSetFile.getCanonicalPath());
		trainedClassifier = makeClassifier(genSet);
	}
	
	/**
	 * Optionally test the classifier using a testing file. For details on the testing file format
	 * see the example cheese2007.test in this project's resources directory. The path 
	 * for this method's parameter is a Maven resource path. See header comment to one of
	 * the initializers for details.
 	 * @param testFileResourcePath resource path to the test file. Example: "myFile.test" 
	 * 			for files in your project's <projectRoot>/src/main/resources directory.
	 */
	public  void test(String testFileResourcePath) {
		String trainFilePath = ArcClassifier.getFilePath(testFileResourcePath);
		test(new File(trainFilePath));
	}
	
	/**
	 * Optionally test the classifier using a testing file. For details on the testing file format
	 * see the example cheese2007.test in this project's resources directory. The path 
	 * for this method's parameter is absolute.
	 * @param testFile absolute path to the test file. 
	 */
	public  void test(File testFile) {
		if (trainedClassifier == null) {
			throw new RuntimeException("Must first train the classifier by calling the train() method.");
		}
		int currTestFileLine = 0;
		for (String line : ObjectBank.getLineIterator(testFile.getAbsolutePath())) {
			Datum<String,String> datum = makeDatumFromLine(line, currTestFileLine);
			currTestFileLine++;
			System.out.println(line + "  ==>  " + trainedClassifier.classOf(datum));
		}		
	}
	
	/**
	 * Optionally save the trained classifier to a file for later retrieval via
	 * method loadClassifier().
	 * @param targetPath absolute path to destination.
	 * @throws IOException
	 */
	public void saveClassifier(String targetPath) throws IOException {
		FileOutputStream fileOutStream  = new FileOutputStream(targetPath);
		ObjectOutputStream objOutStream = new ObjectOutputStream(fileOutStream);
		objOutStream.writeObject(trainedClassifier);
		objOutStream.close();
		fileOutStream.close();
	}
	

	/**
	 * In a new instance of this class: restore a classifier that was previously
	 * trained, and then saved via method saveClassifier(). The path 
	 * for this method's parameter is a Maven resource path. See header comment to one of
	 * the initializers for details.
	 * @param resourcePath resource path to the saved classifier. Example: "myFile.classifier" 
	 * 			for files in your project's <projectRoot>/src/main/resources directory.
	 * @throws IOException
	 */
	public void loadClassifier(String resourcePath) throws IOException {
		String loadFilePath = ArcClassifier.getFilePath(resourcePath);
		loadClassifier(new File(loadFilePath));
	}
	
	/**
	 * In a new instance of this class: restore a classifier that was previously
	 * trained, and then saved via method saveClassifier(). The path 
	 * for this method's parameter is absolute. 
	 * @param sourceFile File object with the absolute path to the saved classifier file.
	 * @throws IOException
	 */
	public void loadClassifier(File sourceFile) throws IOException {
	  FileInputStream fileInStream  = new FileInputStream(sourceFile);
	  ObjectInputStream objInStream = new ObjectInputStream(fileInStream);	  
	  //LinearClassifier<String,String> linearClassifier = ErasureUtils.uncheckedCast(ois.readObject());
	  try {
		  trainedClassifier = ErasureUtils.uncheckedCast(objInStream.readObject());
	  } catch (ClassNotFoundException e) {
		  throw new IOException("Not recognizing the Java class of the classifier being loaded: " + e.getMessage());
	  }
	  objInStream.close();
	  fileInStream.close();
  }	  
  
	/**
	 * Given a 'resource path', return the corresponding absolute 
	 * file name. In Maven the resourcePath is relative to resource root. 
	 * That is <projectRoot>/target/classes, where resources are copied 
	 * from src/main/resources during compilation.
	 * @param resourcePath Path relative to the resource root. Like /myResource.txt
	 * @return the absolute path to the file on this machine, or null if not found. 
	 */
	public static String getFilePath(String resourcePath) {
		ClassLoader classLoader = ClassLoader.getSystemClassLoader();
		URL resourceURL = classLoader.getResource(resourcePath);
		if (resourceURL == null)
			return null;
		return resourceURL.getFile();
	}

	
	//------------------------------  Testing ---------------------------------------------
	
    public static void main( String[] args ) throws IOException {

    	ArcClassifier classifier = new ArcClassifier("cheese2007.prop");
    	classifier.train("cheeseDisease.train");
    	// Test with test file: will print info to console:
    	classifier.test("cheeseDisease.test");
    	System.out.println("------------------------------------");
    	
    	// Save the classifier we just created:
    	File tmpFile = File.createTempFile("classifierTest", ".lcl");
    	// Delete temp file when program exits.
    	tmpFile.deleteOnExit();
    	classifier.saveClassifier(tmpFile.getAbsolutePath());
    	// Make a new ArcClassifier, and rather than training, load
    	// the saved classifier:
    	ArcClassifier classifierNew = new ArcClassifier("cheese2007.prop");
    	classifierNew.loadClassifier(tmpFile);
    	classifierNew.test("cheeseDisease.test");
    }
}
