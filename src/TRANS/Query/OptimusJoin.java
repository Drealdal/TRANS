package TRANS.Query;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.jdom2.JDOMException;

import TRANS.Array.DataChunk;
import TRANS.Array.OptimusArray;
import TRANS.Array.OptimusShape;
import TRANS.Array.OptimusZone;
import TRANS.Array.PID;
import TRANS.Array.Partition;
import TRANS.Array.RID;
import TRANS.Calculator.OptimusCalculator;
import TRANS.Calculator.OptimusDoubleCalculator;
import TRANS.Exceptions.WrongArgumentException;
import TRANS.Protocol.OptimusCalculatorProtocol;
import TRANS.Protocol.OptimusCatalogProtocol;
import TRANS.util.Host;
import TRANS.util.OptimusConfiguration;
import TRANS.util.OptimusDefault;

public class OptimusJoin {
	private OptimusCatalogProtocol ci = null;
	
	public OptimusJoin(OptimusConfiguration conf) throws IOException
	{
		String catalogHost = conf.getString("Optimus.catalog.host", OptimusDefault.CATALOG_HOST);
		int catalogPort = conf.getInt("Optimus.catalog.port", OptimusDefault.CATALOG_PORT);
		ci = (OptimusCatalogProtocol) RPC.waitForProxy(OptimusCatalogProtocol.class,
				OptimusCatalogProtocol.versionID,
				new InetSocketAddress(catalogHost,catalogPort), new Configuration());
	}
	public OptimusCatalogProtocol getCI()
	{
		return this.ci;
	}
	public boolean Jion(OptimusArray a1, OptimusArray a2, OptimusArray a3, OptimusShape start, OptimusShape off,OptimusCalculator c) throws WrongArgumentException
	{
		OptimusZone z1 = ci.openZone(a1.getZid());
		DataChunk chunk = new DataChunk(z1.getSize().getShape(), z1.getPstep().getShape());
		do{
			int id = chunk.getChunkNum();
			PID p = new PID(id);
			
			
			
			try {
				Partition p1 = new Partition(a1.getZid(),a1.getId(),p,new RID(0));
				Partition p2 = new Partition(a1.getZid(),a2.getId(),p,new RID(0));
				Partition p3 = new Partition(a1.getZid(),a3.getId(),p,new RID(0));
			
				Host h = ci.getReplicateHost(p1, new RID( 0 ));
				
				OptimusCalculatorProtocol cp = (OptimusCalculatorProtocol) h.getCalculateProtocol();
				
				BooleanWritable b = cp.JoinArray(p1, p2, p3, c);
				if( b.get() )
				{
					System.out.println("CalCulate OK");
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return false;
			}
			
			
		}while(chunk.nextChunk());
		
		return true;
	}
	public static void main(String []args) throws ParseException, IOException, WrongArgumentException, JDOMException
	{
		CommandLineParser parser = new PosixParser();
		Options options = new Options();
		
		HelpFormatter f = new HelpFormatter();
		 
		options.addOption("h",false,"Print The help infomation");
		options.addOption("c", true, "Configuration directory of catalog");
		options.addOption("op",true,"add Mul Div");
		options.addOption("a1",true,"Name of the array1 zoneName.arrayName");
		options.addOption("a2",true,"Name of the array2 zoneName.arrayName");
		options.addOption("o",true,"Name of the output zoneName.arrayName");
		CommandLine cmd = parser.parse(options, args);
		if(cmd.hasOption("h"))
		{
			f.printHelp("OptimusJion", options);
			System.exit(-1);
		}
		String op = cmd.getOptionValue("op");
		if( op == null )
		{
			f.printHelp("OptimusJion: no operation type", options);
			System.exit(-1);
		}
		String a1 = cmd.getOptionValue("a1");
		String a2 = cmd.getOptionValue("a2");
		String o = cmd.getOptionValue("o");
		if(a1 == null || a2 == null || o == null)
		{
			f.printHelp("OptimusJion: no a1 || a2 time", options);
			System.exit(-1);
		}
		OptimusDoubleCalculator c = null;
		if(op.equals("add"))
		{
			c = new OptimusDoubleCalculator('+');
		}else if(op.equals("sub")){
			c = new OptimusDoubleCalculator('-');
		}else if(op.equals("mul"))
		{
			c = new OptimusDoubleCalculator('*');
		}else{
			System.out.println("Not support operator");
			System.exit(-1);
		}
		
		String []z1name = a1.split("\\.");
		String []z2name = a2.split("\\.");
		String []z3name = o.split("\\.");
		if(z1name.length != 2 || z2name.length != 2 || z3name.length != 2)
		{
			f.printHelp("OptimusJion: array name", options);
			System.exit(-1);
		}
		String confPath = cmd.getOptionValue("c");
		if(confPath == null)
		{
			confPath = "./conf";
		}
		
		OptimusJoin join = new OptimusJoin(new OptimusConfiguration(confPath));
		OptimusCatalogProtocol ci = join.getCI();
		OptimusZone zone1 = ci.openZone(new Text(z1name[0]));
		OptimusZone zone2 = ci.openZone(new Text(z2name[0]));
		if(zone1 == null || zone2 == null )
		{
			f.printHelp("OptimusJion: Zone name wrong", options);
			System.exit(-1);
		}
		OptimusArray array1 = ci.openArray(zone1.getId(), new Text(z1name[1]));
		OptimusArray array2 = ci.openArray(zone2.getId(), new Text(z2name[1]));
		
		if( array1 == null || array2 == null )
		{
			f.printHelp("OptimusJion: Array name wrong", options);
			System.exit(-1);
		}
		OptimusZone zone3 = ci.openZone(new Text(z3name[0]));
		if(zone3 == null)
		{
			zone3 = zone1;
		}
		OptimusArray array3 = null;
		try {
			array3 = ci.openArray(zone3.getId(), new Text(z3name[1]));
		}catch(Exception e){
			if(array3 == null)
			{
				array3 = ci.openArray(zone3.getId(), new Text(z3name[1]));
			}
		}
		
		if( join.Jion(array1, array2, array3, null, null, c) )
		{
			System.out.println("Join Success");
		}
	}
}
