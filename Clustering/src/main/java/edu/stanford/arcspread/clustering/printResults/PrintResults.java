package edu.stanford.arcspread.clustering.printResults;

import org.apache.commons.cli2.CommandLine;
import org.apache.commons.cli2.Group;
import org.apache.commons.cli2.Option;
import org.apache.commons.cli2.OptionException;
import org.apache.commons.cli2.builder.ArgumentBuilder;
import org.apache.commons.cli2.builder.DefaultOptionBuilder;
import org.apache.commons.cli2.builder.GroupBuilder;
import org.apache.commons.cli2.commandline.Parser;
import org.apache.hadoop.conf.Configuration;
import org.apache.mahout.common.CommandLineUtil;
import org.apache.mahout.common.commandline.DefaultOptionCreator;

public class PrintResults {

	/**
	 * @param args
	 * @throws OptionException 
	 */
	public static void main(String[] args) throws OptionException 
	{
		Configuration conf = new Configuration();
		
		@SuppressWarnings("unused")
		ArgumentBuilder			argBuilder = new ArgumentBuilder();
		DefaultOptionBuilder	optBuilder = new DefaultOptionBuilder();
		GroupBuilder			grpBuilder = new GroupBuilder();
		
		//basic input, output and help options
		Option inputOpt		= DefaultOptionCreator.inputOption().create();
		Option outputOpt	= DefaultOptionCreator.outputOption().create();
		Option helpOpt		= optBuilder.withLongName("help").withShortName("h").withDescription("Print out help").create();
		
		Group group = grpBuilder.withName("Options").withOption(inputOpt).withOption(outputOpt).create();
		try
		{
			Parser parser = new Parser();
			parser.setGroup(group);
			CommandLine cmdLine = parser.parse(args);
			
			//if help is wanted or no args provided, print help
			if(args.length == 0 || cmdLine.hasOption(helpOpt))
			{
				CommandLineUtil.printHelp(group);
				return;
			}
			
			String input	= cmdLine.getValue(inputOpt).toString();
			String output	= cmdLine.getValue(outputOpt).toString();
			
			PrintDocTopics.print(conf, input, output);
		}
		catch(OptionException e)
		{
			CommandLineUtil.printHelp(group);
			throw e;
		}
		
	}

}
