REGISTER $PIG_HOME/contrib/piggybank/java/piggybank.jar;
REGISTER contrib/PigIR.jar;

docs = LOAD 'gov-03-2009:11'
USING pigir.webbase.WebBaseLoader()
AS (url:chararray,
	 date:chararray,
	 pageSize:int,
	 position:int,
	 docidInCrawl:int,
	 httpHeader,
	 content:chararray);

rawIndex = FOREACH docs GENERATE 
			pigir.pigudf.IndexOneDoc(pigir.pigudf.GetLUID(), content);

flatRawIndex = FOREACH rawIndex GENERATE flatten($0);
			
index = ORDER flatRawIndex BY $0;

DUMP index;
