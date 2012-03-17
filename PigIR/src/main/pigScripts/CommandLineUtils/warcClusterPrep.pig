/* 
   Given a WARC file, extract the contained Web pages,
   extract the title of each page, and store each in its own file. 
   Each file is named a unique UUID.  We also output a map of 
   this UUID to the respective warc record ID.

   It is convenient to start this script via warcClusterPrep,
   or---less conveniently---via pigrun. warcClusterPrep calls
   pigrun.

   Environment assumptions (all taken care of by pigrun, if 
    you initialized it):
    
      * $PIG_HOME points to root of Pig installation
      * $USER_CONTRIB points to location of PigIR.jar
      * $USER_CONTRIB points to location of jsoup-1.5.2.jar
      
   $PIG_HOME and $USER_CONTRIB are assumed to be passed in
   via -param command line parameters. The pigrun script that
   is used by warcClusterPrep takes care of this. 

   The warcClusterPrep script constructs these parameters from its
   command line parameters.
      
*/       

--REGISTER $PIG_HOME/contrib/piggybank/java/piggybank.jar;
REGISTER $USER_CONTRIB/PigIR.jar;
REGISTER $USER_CONTRIB/jsoup-1.5.2.jar

define titleText edu.stanford.pigir.pigudf.ExtractSingleHTMLTag('title');

docs = LOAD '$WARC_FILE'
		USING edu.stanford.pigir.warc.WarcLoader
       AS (warcRecordId:chararray, contentLength:int, date:chararray, warc_type:chararray,
           optionalHeaderFlds:bytearray, content:chararray);
  
titles = FOREACH docs GENERATE edu.stanford.pigir.pigudf.GetUUID(), warcRecordId, edu.stanford.pigir.pigudf.FilterClusteringStopwords(titleText(content));
STORE titles INTO '$CLUSTER_PREP_DEST/To_Cluster' USING edu.stanford.pigir.pigudf.TupleMultiStorage('$CLUSTER_PREP_DEST/To_Cluster', '0', '2');
STORE titles INTO '$CLUSTER_PREP_DEST/ID_Map' USING PigStorage(',');




