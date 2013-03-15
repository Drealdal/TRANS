package TRANS.MR.Tools;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import TRANS.MR.Average.Mapper.AverageMapper;
import TRANS.MR.Average.Reducer.AverageReducer;
import TRANS.MR.Combiner.AverageCombiner;
import TRANS.MR.Median.StrideResult;
import TRANS.MR.Median.TRANSMedianInputFormat;
import TRANS.MR.io.AverageResult;

public class Median {

	public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    
    
	CommandLineParser parser = new PosixParser();
	Options options = new Options();
	
	HelpFormatter f = new HelpFormatter();
	 
	options.addOption("name",true,"name of the array zoneName.arrayName");
	options.addOption("start",true,"start to read");
	options.addOption("off",true,"shape to read, start.length == off.lengh");
	options.addOption("o",true,"output path");
	options.addOption("push",false,"Push calculate to datanode");
	
	CommandLine cmd = parser.parse(options, args);
	if(cmd.hasOption("h"))
	{
		f.printHelp("Reader printh", options);
		System.exit(-1);
	}
	String confPath = cmd.getOptionValue("c");
	if(confPath == null)
	{
		confPath = "./conf";
	}
	
	String name = cmd.getOptionValue("name");
	String start = cmd.getOptionValue("start");
	String off = cmd.getOptionValue("off");
	String out = cmd.getOptionValue("o");
	
	if( name == null || start == null || off == null || out == null )
	{
		f.printHelp("Reader has null", options);
		System.exit(-1);
	}
	
	String [] names = name.split("\\.");
	if(names.length != 2)
	{
	
		f.printHelp("Reader zoneName.arrayName", options);
		System.exit(-1);
	}
	String zoneName = names[0];
	String arrayName = names[1];
	
	String []starts = start.split(",");
	String []offs = off.split(",");
	if(starts.length != offs.length)
	{
		f.printHelp("Reader, start != off", options);
		System.exit(-1);
	}
    
    conf.set("TRANS.zone.name", zoneName);
    conf.set("TRANS.array.name", arrayName);
    conf.set("TRANS.range.start",start);
    conf.set("TRANS.range.offset",off);
    Job job = null;
 
    job = new Job(conf, "Median");
    job.setInputFormatClass(TRANSMedianInputFormat.class);
    job.setJarByClass(Median.class);

    job.setMapperClass(TRANS.MR.Median.Mapper.MedianMapper.class);
	job.setReducerClass(TRANS.MR.Median.Reducer.MedianReducer.class);
	
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(StrideResult.class);
    
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(DoubleWritable.class);
    //    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(out));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
