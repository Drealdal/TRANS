package TRANS.Client.Reader;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.jdom2.JDOMException;

import TRANS.Array.OptimusZone;
import TRANS.Client.ZoneClient;
import TRANS.Exceptions.WrongArgumentException;
import TRANS.util.OptimusConfiguration;

public class OptimusReader {

	/**
	 * @param args
	 * @throws ParseException 
	 * @throws IOException 
	 * @throws JDOMException 
	 * @throws WrongArgumentException 
	 */
	public static void main(String[] args) throws ParseException, WrongArgumentException, JDOMException, IOException {
		
		
		CommandLineParser parser = new PosixParser();
		Options options = new Options();
		
		HelpFormatter f = new HelpFormatter();
		 
		options.addOption("h",false,"Print The help infomation");
		options.addOption("c", true, "Configuration directory of catalog");
		options.addOption("name",true,"name of the array zoneName.arrayName");
		options.addOption("start",true,"start to read");
		options.addOption("off",true,"shape to read, start.length == off.lengh");
		options.addOption("p",false,"print the data");
		options.addOption("fake",false,"Fake read");
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
		
		if( name == null || start == null || off == null )
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
		int [] spoint = new int [starts.length];
		int [] opoint = new int [starts.length];
		
		for( int i = 0 ; i < spoint.length; i++ )
		{
			spoint[i] = Integer.parseInt(starts[i]);
			opoint[i] = Integer.parseInt(offs[i]);
		}
		OptimusConfiguration conf = new OptimusConfiguration(confPath);
		PartitionReader reader = new PartitionReader(conf);
		if(cmd.hasOption("fake"))
		{
			reader.setDoRead(false);
		}
		ZoneClient zclient = new ZoneClient(conf);
		
		OptimusZone zone = zclient.openZone(zoneName);
		if(zone == null)
		{
			System.out.print("UnCreated zone or unknown error happened");
			System.exit(-1);
		}
		double [] data = reader.readData(zone,arrayName, spoint, opoint);
		if( cmd.hasOption("p"))
		{
			for(int i = 0 ; i < data.length; i++)
			{
				System.out.println(data[i]);
			}
		}
		reader.printMatrix();
	}

}
