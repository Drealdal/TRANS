package TRANS.MR.Tools;

import java.util.Vector;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import TRANS.Array.ArrayID;
import TRANS.Array.DataChunk;
import TRANS.Array.OptimusArray;
import TRANS.Array.OptimusZone;
import TRANS.Client.ZoneClient;
import TRANS.MR.Average.Mapper.AverageMapper;
import TRANS.MR.Average.Reducer.AverageReducer;
import TRANS.MR.Combiner.AverageCombiner;
import TRANS.MR.Median.StrideResult;
import TRANS.MR.Median.StripeMedianResult;
import TRANS.MR.Median.TRANSMedianInputFormat;
import TRANS.MR.Median.combiner.MedianPartitioner;
import TRANS.MR.io.AverageResult;
import TRANS.Protocol.OptimusCatalogProtocol;
import TRANS.util.OptimusConfiguration;

public class Median {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		CommandLineParser parser = new PosixParser();
		Options options = new Options();

		HelpFormatter f = new HelpFormatter();

		options.addOption("name", true, "name of the array zoneName.arrayName");
		options.addOption("start", true, "start to read");
		options.addOption("off", true,
				"shape to read, start.length == off.lengh");
		options.addOption("stride", true, "stride of the operation");
		options.addOption("opartition", true,
				"Partition Strategy for the output");
		options.addOption("ostrategy", true,
				"Partition Strategy for the ouptut array");
		options.addOption("oarray", true, "output array name");
		options.addOption("push", false, "Push calculate to datanode");
		options.addOption("o",true,"OutputDirectory");

		CommandLine cmd = parser.parse(options, args);
		if (cmd.hasOption("h")) {
			f.printHelp("Reader printh", options);
			System.exit(-1);
		}
		String confPath = cmd.getOptionValue("c");
		if (confPath == null) {
			confPath = System.getenv("OPTIMUS_CONF");
		} else if (confPath == null) {
			confPath = "./conf";
		}

		String name = cmd.getOptionValue("name");
		String start = cmd.getOptionValue("start");
		String off = cmd.getOptionValue("off");
		String stride = cmd.getOptionValue("stride");

		String out = cmd.getOptionValue("oarray");
		String outpath = cmd.getOptionValue("o");

		if (name == null || start == null || off == null || stride == null
				|| out == null) {
			f.printHelp("Reader has null", options);
			System.exit(-1);
		}

		String[] names = name.split("\\.");
		String[] output = out.split("\\.");
		if (names.length != 2) {

			f.printHelp("Reader zoneName.arrayName", options);
			System.exit(-1);
		}
		String zoneName = names[0];
		String arrayName = names[1];

		String outZone = output[0];
		String outName = output[1];

		String[] starts = start.split(",");
		String[] offs = off.split(",");
		if (starts.length != offs.length) {
			f.printHelp("Reader, start != off", options);
			System.exit(-1);
		}

		ZoneClient zcreater = new ZoneClient(new OptimusConfiguration(confPath));
		OptimusZone zone = zcreater.openZone(outZone);
		if (zone == null) {
			int[] ov = new int[starts.length];
			String[] strides = stride.split(",");

			int[] o = new int[starts.length];
			int[] str = new int[starts.length];
			for (int i = 0; i < str.length; i++) {
				o[i] = Integer.parseInt(offs[i]);
				str[i] = Integer.parseInt(strides[i]);
				ov[i] = o[i] / str[i];
			}

			String[] oshape = cmd.getOptionValues("ostrategy");
			String pStep = cmd.getOptionValue("opartition");
			Vector<int[]> shapes = new Vector<int[]>();
			if (oshape == null || pStep == null) {
				f.printHelp("TRANS Median", options);
				System.exit(-1);
			}
			String[] ps = pStep.split(",");
			int[] psize = new int[starts.length];
			for (int i = 0; i < starts.length; i++) {
				psize[i] = Integer.parseInt(ps[i]);
			}

			for (String s : oshape) {
				String[] a = s.split(",");
				if (a.length != starts.length) {
					System.out.println("Wrong chunk strategy");
					System.exit(-1);
				}
				int[] tmp = new int[starts.length];
				int i = 0;
				for (String ts : a) {
					tmp[i++] = Integer.parseInt(ts);
				}
				shapes.add(tmp);
			}
			int[] sshape = new int[starts.length];
			sshape[starts.length - 1] = ov[starts.length - 1];
			for (int i = 0; i < starts.length - 1; i++) {
				sshape[i] = 1;
			}
			shapes.add(sshape);
			zone = zcreater.createZone(outZone, ov, psize, shapes);
			
		}
		OptimusCatalogProtocol ci = zcreater.getCi();
		ArrayID array = ci.createArray(zone.getId(), new Text(outName),
				new FloatWritable(0));
		int []pshape = zone.getPstep().getShape();
		String p = "";
		p+=pshape[0];
		for(int i=1; i < pshape.length;i++)
		{
			p+=","+pshape[i];
		}
		DataChunk d = new DataChunk(zone.getSize().getShape(),zone.getPstep().getShape());
		
		int reducerNumber = d.getTotalChunk();
		conf.set("TRANS.output.array.pshape", p);
		conf.setInt("TRANS.output.array.zoneid",zone.getId().getId());
		conf.setInt("TRANS.output.array.rnum", zone.getStrategy().getShapes().size() -1);
		conf.setInt("TRANS.output.array.id", array.getArrayId());
		conf.set("TRANS.zone.name", zoneName);
		conf.set("TRANS.array.name", arrayName);
		conf.set("TRANS.range.start", start);
		conf.set("TRANS.range.stride", stride);
		conf.set("TRANS.range.offset", off);
		conf.set("TRANS.output.name", out);
		Job job = null;

		job = new Job(conf, "Median");
		job.setInputFormatClass(TRANSMedianInputFormat.class);
		job.setJarByClass(Median.class);
		job.setCombinerClass(TRANS.MR.Median.combiner.MedianCombiner.class);
		job.setMapperClass(TRANS.MR.Median.Mapper.MedianMapper.class);
		job.setReducerClass(TRANS.MR.Median.Reducer.MedianReducer.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(StripeMedianResult.class);
		job.setPartitionerClass(MedianPartitioner.class);
		//job.setp
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setNumReduceTasks(reducerNumber);
		// FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(outpath));
		
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
