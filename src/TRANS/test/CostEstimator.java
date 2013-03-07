package TRANS.test;

import java.io.IOException;
import java.util.Vector;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import TRANS.Client.Reader.PartitionReader;
import TRANS.Exceptions.WrongArgumentException;

public class CostEstimator {
public static void main(String []args) throws ParseException, IOException, WrongArgumentException
{
	CommandLineParser parser = new PosixParser();
	
	Options options = new Options();
	HelpFormatter f = new HelpFormatter();
	options.addOption("h",false,"Print The help infomation");
	options.addOption("s", true, "Chunk Strategy of the partition");
	options.addOption("p",true,"Partition strategy");
	options.addOption("v",true,"Varible size");
	options.addOption("start",true,"start");
	options.addOption("off",true,"off");
	
	
	CommandLine cmd = parser.parse(options, args);
	
	if( cmd.hasOption("h"))
	{
         f.printHelp("NetCDF Loader", options);
         System.exit(-1);
	}
	String p = cmd.getOptionValue("p");
	String v = cmd.getOptionValue("v");
	String start = cmd.getOptionValue("start");
	String off = cmd.getOptionValue("off");
	if(p == null)
	{
		 f.printHelp("NetCDF Loader", options);
         System.exit(-1);
	}
	String [] ps = p.split(",");
	String [] vs = v.split(",");
	String [] starts = start.split(",");
	String [] offs = off.split(",");
	
	int d = ps.length;
	int [] pshape = new int [d];
	int [] vshape = new int [d];
	int [] sss = new int[d];
	int [] ooo = new int[d];
	int i = 0;
	for( i = 0; i < d; i++)
	{
		pshape[i] = Integer.parseInt(ps[i]);
		vshape[i] = Integer.parseInt(vs[i]);
		sss[i] = Integer.parseInt(starts[i]);
		ooo[i] = Integer.parseInt(offs[i]);
	}
	
	String [] ss = cmd.getOptionValues("s");
	if( ss == null )
	{
		 f.printHelp("NetCDF Loader", options);
         System.exit(-1);
	}
	Vector<int []> shapes = new Vector<int []>();
	for(String s:ss)
	{
		String [] a = s.split(",");
		if(a.length != d)
		{
			System.out.println("Wrong chunk strategy");
			System.exit(-1);
		}
		int [] tmp = new int [d];
		i = 0;
		for(String ts: a)
		{
			tmp[i++] = Integer.parseInt(ts);
		}
		shapes.add(tmp);
	}
	shapes.add(pshape);
	PartitionReader reader = new PartitionReader();
	reader.CostEstimate(vshape, pshape, shapes, sss, ooo);
	

}
}
