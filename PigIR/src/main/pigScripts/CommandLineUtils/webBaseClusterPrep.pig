/* 
   Given a WebBase crawl, and optionally a number of pages,
   start, and stop sites, extract the contained Web pages,
   extract the HTML title, and store each in its own file
   under $CLUSTER_HOME/To_Cluster/$CRAWL_NAME. 
         
   By default output is to HDFS directory /user/paepcke/To_Cluster

   You should run this script via webBaseClusterPrep:
   
    Environment expectations:
      * $PIG_HOME points to root of Pig installation
      * $USER_CONTRIB points to location of PigIR.jar
      * $USER_CONTRIB points to location of jsoup-1.5.2.jar

   $PIG_HOME and $USER_CONTRIB are assumed to be passed in
   via -param command line parameters. The pigrun script that
   is used by webBaseWordCount takes care of this. Additionally,
   the following env vars must be passed in via -param:
   
       $CRAWL_SOURCE=crawl source, page numbers, start/stop sites as per examples above
       $CLUSTER_PREP_DEST  : Full destination path for cluster preparation
	   		Example: /home/doe/gov-03-2007_wordCount.cnt

   The webBaseClusterPrep script constructs these parameters from its
   command line parameters.
*/   

--REGISTER $PIG_HOME/contrib/piggybank/java/piggybank.jar;
REGISTER $USER_CONTRIB/PigIR.jar;
REGISTER $USER_CONTRIB/jsoup-1.5.2.jar

define titleText edu.stanford.pigir.pigudf.ExtractSingleHTMLTag('title');

docs = LOAD '$CRAWL_SOURCE'
	USING edu.stanford.pigir.webbase.WebBaseLoader()
	AS (url:chararray,
	    date:chararray,
	 	pageSize:int,
	 	position:int,
	 	docIDInCrawl:chararray,
	 	httpHeader,
	 	content:chararray);

--extract html title and store each into its own text file
titles = FOREACH docs GENERATE CONCAT(CONCAT('$CRAWL_NAME', '_'), docIDInCrawl), edu.stanford.pigir.pigudf.FilterClusteringStopwords(titleText(content));
STORE titles INTO '$CLUSTER_PREP_DEST/To_Cluster/$CRAWL_NAME' USING edu.stanford.pigir.pigudf.TupleMultiStorage('$CLUSTER_PREP_DEST/To_Cluster/$CRAWL_NAME', '0', '1');


