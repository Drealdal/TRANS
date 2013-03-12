package TRANS.Client.creater;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import TRANS.Array.DataChunk;

import ucar.ma2.InvalidRangeException;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;
public class NetcdfScanner implements OptimusScanner {

	public static final Log LOG = LogFactory.getLog(NetcdfScanner.class.getName());
	private NetcdfFile nc = null; 
	private int [] chunkStep = null;
	public NetcdfScanner(int []chunkStep)
	{
		this.chunkStep = chunkStep;
		
	}
	
	public int open(String path) {
		
		if(path == null)
		{
			LOG.error("Open null path");
			return -1;
		}
		try {
			nc = NetcdfFile.open(path);
			if( nc == null ){
				LOG.error("OPEN FILE FAILURE");
				return -1;
			}
		} catch (IOException e) {
		
			e.printStackTrace();
		}
		return 0;
	}

	
	public   double [] readChunkDouble(DataChunk chunk, String name) {
		
		Variable v = nc.findVariable(name);
		if( v == null )
		{
			LOG.error("FILE varible "+name+" failure");
			return null;
		}
		try {
				return  (double [] )v.read(chunk.getStart(), chunk.getChunkSize()).copyTo1DJavaArray();
			} catch (IOException e) {
				
				e.printStackTrace();
			} catch (InvalidRangeException e) {
			
				e.printStackTrace();
			}
		return null;
	}

	public int [] getShape(String name)
	{
		
		return nc.findVariable(name).getShape();
	}
	@SuppressWarnings("deprecation")
	public List<String> getVaribles(int []shape)
	{
		List<String> vnames = new ArrayList<String>();
		
		List<Variable> lv =  nc.getVariables();
		for(Variable v: lv)
		{
		
			if(v.getShape().length == shape.length)
			{
				vnames.add(v.getName());
			}
		}
		return vnames;
	}
	
	public int [] getStep()
	{
		return this.chunkStep;
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		int [] chunkStep = {2,3,2};
		NetcdfScanner scanner = new NetcdfScanner(chunkStep);
		scanner.open("example.nc");
		
		
		//	scanner.readChunk(chunk, name)
	}

}
