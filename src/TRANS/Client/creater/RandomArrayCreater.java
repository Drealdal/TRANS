package TRANS.Client.creater;

import java.io.IOException;
import java.util.Vector;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.jdom2.JDOMException;

import TRANS.Array.DataChunk;
import TRANS.Array.OptimusZone;
import TRANS.Client.ArrayCreater;
import TRANS.Client.NetcdfLoader;
import TRANS.Client.ZoneClient;
import TRANS.Exceptions.WrongArgumentException;
import TRANS.util.OptimusConfiguration;
import TRANS.util.OptimusDefault;

public class RandomArrayCreater {
	private DataChunk chunk = null;
	private OptimusConfiguration conf = null;
	
	private ArrayCreater creater = null;
	private OptimusScanner scanner = null;
	java.util.Vector<int []> shapes = null;
	
	private OptimusZone zone = null;
	private int thread_num = 1;
	private int [] vsize = null;
	private long dataSize = 0;
	private long timeUsed = 0;
	public RandomArrayCreater(OptimusConfiguration conf,String zonename, int []Vshape, 
			int []pshape, Vector<int[]> strategy, int thread_num) throws IOException
	{
		scanner = new OptimusRandomScanner();
	
		vsize  = Vshape;
		chunk = new DataChunk( vsize,pshape);
		this.shapes = strategy;	
		
		ZoneClient zcreater = new ZoneClient(conf);
		if(( zone = zcreater.openZone(zonename)) == null)
		{
			zone = zcreater.createZone(zonename,chunk.getVsize(),chunk.getChunkStep(),this.shapes);	
		}
		this.conf = conf;
		this.thread_num = thread_num;
	}
	public static void main(String[] args) throws ParseException, IOException, WrongArgumentException, JDOMException, InterruptedException {

		CommandLineParser parser = new PosixParser();
		
		Options options = new Options();
		HelpFormatter f = new HelpFormatter();
		 
		options.addOption("h",false,"Print The help infomation");
		options.addOption("c", true, "Configuration directory of catalog");
		options.addOption("s", true, "Chunk Strategy of the partition");
		options.addOption("p",true,"Partition strategy");
		options.addOption("v",true,"Varible name");
		options.addOption("vs",true,"Varible shape");
		options.addOption("z",true,"Zone name");
		options.addOption("Thread",true,"Thread number used");
		CommandLine cmd = parser.parse(options, args);
		if( cmd.hasOption("h"))
		{
             f.printHelp("NetCDF Loader", options);
             System.exit(-1);
		}
		String p = cmd.getOptionValue("p");
		String vs = cmd.getOptionValue("vs");
		if(p == null || vs == null)
		{
			 f.printHelp("NetCDF Loader p:"+p +"vs:" + vs, options);
             System.exit(-1);
		}
		String [] ps = p.split(",");
		String [] vss = vs.split(",");
		if(ps.length != vss.length )
		{
			 f.printHelp("NetCDF Loader p:"+p +"vs:" + vs, options);
             System.exit(-1);
			
		}
		int d = ps.length;
		int [] pshape = new int [d];
		int [] vshape = new int [d];
		int i = 0;
		for( i = 0 ; i < ps.length; i++)
		{
			pshape[i] = Integer.parseInt(ps[i]);
			vshape[i] = Integer.parseInt(vss[i]);
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
		String confDir = cmd.getOptionValue("c",OptimusDefault.OPTIMUSCONFIG);
	
		String zonename = cmd.getOptionValue("z");
		String vname = cmd.getOptionValue("v");		
		String tstring = cmd.getOptionValue("Thread");
		int tnum = 1;
		if(tstring != null)
		{
			tnum = Integer.parseInt(tstring);
		}
		if( zonename == null || vname == null)
		{
			f.printHelp("NetCDF Loader", options);
            System.exit(-1);
		}

		RandomArrayCreater creater = new RandomArrayCreater(new OptimusConfiguration(confDir),zonename,vshape,pshape,shapes,tnum);
		creater.uploadArray(vname, 0);
		creater.printMarix();
	}
	public void uploadArray(String vname,float defaultValue) throws IOException, WrongArgumentException, InterruptedException
	{
		creater = new ArrayCreater(conf, zone, this.shapes.get(this.shapes.size() - 1),vname,this.thread_num,defaultValue);
		int i ;
		int []srcShape = new int [chunk.getChunkStep().length];
		long tmpSize  = 1;
		for( i = 0 ; i < chunk.getChunkStep().length ; ++i)
		{
			srcShape[i] = chunk.getChunkStep()[i];
			tmpSize *= this.vsize[i];
		}
		this.dataSize += tmpSize;
		srcShape[i-1] = 1;
		creater.create();
		long btime = System.currentTimeMillis();
		do{
		//float []data = new float [chunk.getSize()];
		//TODO how to parralel reading from source
			creater.createPartition(scanner,chunk,vname);
			
		}while(this.chunk.nextChunk());
		try {
			while(!creater.close(30, TimeUnit.SECONDS))
			{
				//System.out.println("Waiting for the creating of the array");
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		long etime = System.currentTimeMillis();
		this.timeUsed += etime - btime;
	}
	public void printMarix()
	{
		System.out.println("thread: time : Byte  " + this.thread_num+":"+this.timeUsed +" : " + (this.dataSize * 8)/(1024*1024) +"MB");
	}

}
