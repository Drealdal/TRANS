package TRANS.Client;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.List;
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
import TRANS.Array.OptimusShape;
import TRANS.Array.OptimusZone;
import TRANS.Client.creater.NetcdfScanner;
import TRANS.Exceptions.WrongArgumentException;
import TRANS.util.OptimusConfiguration;
import TRANS.util.OptimusData;
import TRANS.util.OptimusDefault;

/**
 * @author foryee
 *
 */
public class NetcdfLoader {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws UnknownHostException 
	 * @throws JDOMException 
	 * @throws WrongArgumentException 
	 * @throws ParseException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws UnknownHostException, IOException, WrongArgumentException, JDOMException, ParseException, InterruptedException {
		
		CommandLineParser parser = new PosixParser();
		
		Options options = new Options();
		HelpFormatter f = new HelpFormatter();
		 
		options.addOption("h",false,"Print The help infomation");
		options.addOption("c", true, "Configuration directory of catalog");
		options.addOption("s", true, "Chunk Strategy of the partition");
		options.addOption("p",true,"Partition strategy");
		options.addOption("v",true,"Varible name");
		options.addOption("f",true,"NetCDF file path");
		options.addOption("z",true,"Zone name");
		options.addOption("Thread",true,"Thread number used");
		options.addOption("A",false,"Upload all varibles");
		CommandLine cmd = parser.parse(options, args);
		
		if( cmd.hasOption("h"))
		{
             f.printHelp("NetCDF Loader", options);
             System.exit(-1);
		}
		String p = cmd.getOptionValue("p");
		if(p == null)
		{
			 f.printHelp("NetCDF Loader", options);
             System.exit(-1);
		}
		String [] ps = p.split(",");
		int d = ps.length;
		int [] pshape = new int [d];
		int i = 0;
		for(String s:ps)
		{
			pshape[i++] = Integer.parseInt(s);
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
		String path = cmd.getOptionValue("f");
		String zonename = cmd.getOptionValue("z");
		String vname = cmd.getOptionValue("v");		
		String tstring = cmd.getOptionValue("Thread");
		int tnum = 1;
		if(tstring != null)
		{
			tnum = Integer.parseInt(tstring);
		}
		if(path == null || zonename == null || vname == null)
		{
			f.printHelp("NetCDF Loader", options);
            System.exit(-1);
		} 
		System.out.println("Configuration Dir:" + confDir);
		
		NetcdfLoader loader = new NetcdfLoader(new OptimusConfiguration(confDir),zonename,path,pshape,
				shapes,vname,tnum);
	//	loader.uploadArray(vname);
		if(!cmd.hasOption("A"))
		{
			loader.uploadArray(vname, 0.0f);
		}else{
			loader.uploadAll();
		}
		loader.printMarix();
	}
	private DataChunk chunk = null;
	private OptimusConfiguration conf = null;
	
	private ArrayCreater creater = null;
	private NetcdfScanner scanner = null;
	java.util.Vector<int []> shapes = null;
	
	private OptimusZone zone = null;
	private int thread_num = 1;
	private int [] vsize = null;
	private long dataSize = 0;
	private long timeUsed = 0;
	
	/**
	 * @param conf: configuration directory
	 * @param zonename: the name of the zone
	 * @param path: path of the netcdf file
	 * @param partitionStep: partition strategy
	 * @param shapes: chunk strategy
	 * @param vname: the varible represent of the zone array size
	 * @throws UnknownHostException
	 * @throws IOException
	 */
	NetcdfLoader(OptimusConfiguration conf,String zonename,
			String path, int []partitionStep,Vector<int []> shapes,String vname,int thread_num) throws UnknownHostException, IOException
	{
		scanner = new NetcdfScanner(partitionStep);
		scanner.open(path);
		vsize  = scanner.getShape(vname);
		chunk = new DataChunk( vsize,partitionStep);
		this.shapes = shapes;	
		
		ZoneClient zcreater = new ZoneClient(conf);
		if(( zone = zcreater.openZone(zonename)) == null)
		{
			zone = zcreater.createZone(zonename,chunk.getVsize(),chunk.getChunkStep(),this.shapes);	
		}
		this.conf = conf;
		this.thread_num = thread_num;
	}
	
	
	public OptimusZone getZone() {
		return zone;
	}

	public void setZone(OptimusZone zone) {
		this.zone = zone;
	}

	public void uploadArray(String vname,float defaultValue) throws UnknownHostException, IOException, WrongArgumentException, InterruptedException
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
	
	public void uploadAll() throws IOException, IOException, WrongArgumentException, InterruptedException
	{
		List<String> vnames = scanner.getVaribles(this.chunk.getVsize());
		for(String s: vnames)
		{
			chunk.reset();
			this.uploadArray(s,0);
		}
		System.out.println("Data Upload over");
	}
	public void printMarix()
	{
		System.out.println("thread: time : Byte  " + this.thread_num+":"+this.timeUsed +" : " + (this.dataSize * 8)/(1024*1024) +"MB");
	}
}
