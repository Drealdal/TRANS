package TRANS.Client.Reader;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Set;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;

import TRANS.OptimusDataManager;
import TRANS.Array.DataChunk;
import TRANS.Array.OptimusArray;
import TRANS.Array.OptimusShape;
import TRANS.Array.OptimusZone;
import TRANS.Array.PID;
import TRANS.Array.Partition;
import TRANS.Array.RID;
import TRANS.Exceptions.WrongArgumentException;
import TRANS.Protocol.OptimusCatalogProtocol;
import TRANS.Protocol.OptimusDataProtocol;
import TRANS.util.Host;
import TRANS.util.OptimusConfiguration;
import TRANS.util.OptimusDefault;

public class PartitionReader {
	
	private long totalSize = 0;
	private boolean doRead = true; 
	private long time = 0;
	private OptimusCatalogProtocol ci = null;
	public PartitionReader(){};
	public PartitionReader(OptimusConfiguration conf) throws IOException
	{
		String catalogHost = conf.getString("Optimus.catalog.host", OptimusDefault.CATALOG_HOST);
		int catalogPort = conf.getInt("Optimus.catalog.port", OptimusDefault.CATALOG_PORT);
		
		ci = (OptimusCatalogProtocol) RPC.waitForProxy(OptimusCatalogProtocol.class,
				OptimusCatalogProtocol.versionID,
				new InetSocketAddress(catalogHost,catalogPort), new Configuration());
	}
	
	public double [] readData(OptimusZone zone,String name, int []start, int []off) throws IOException, WrongArgumentException
	{
		long b = System.currentTimeMillis();
		OptimusArray array = ci.openArray(zone.getId(),new Text(name));
		
		if(array.getId() == null)
		{
			throw new WrongArgumentException("ArrayName","Not find array");
		}
		DataChunk chunk = new DataChunk(zone.getSize().getShape(),zone.getPstep().getShape());
		Set<DataChunk> chunks = chunk.getAdjacentChunks(start, off);
		int rsize = 1;
		for(int i = 0 ; i < start.length; i++)
			rsize *= off[i];
		
		double [] rdata = null;
		if(this.doRead )
			rdata = new double[rsize];
	
		
		Vector<int []> strategy = zone.getStrategy().getShapes();
		int [] count = new int [strategy.size() - 1];
		for(DataChunk c:chunks)
		{
			int id = c.getChunkNum();
			PID p = new PID(id);
			
	
			
			int dataSize = java.lang.Integer.MAX_VALUE;
			int take = -1;
		
			int [] nstart = new int [start.length];
			int [] noff = new int [start.length];
			// start in the partition
			int [] rstart = new int [start.length];
		
			
			int [] cstart = c.getStart();
			int [] coff = c.getChunkStep();
			
			for(int i = 0 ; i < start.length; i++)
			{
				nstart[i] = start[i] > cstart[i] ? start[i] : cstart[i];
				noff[i] = start[i] + off[i] < cstart[i] + coff[i] ? start[i] + off[i]:cstart[i] + coff[i]; 
				noff[i] -= nstart[i];
				
				rstart[i] =nstart[i] - cstart[i]; // 
			}
			
			for(int i = 0; i < strategy.size() - 1; i++)
			{
				DataChunk tmp = new DataChunk(zone.getPstep().getShape(),strategy.get(i));
				tmp.setStart(c.getStart());
				
				Set<DataChunk> scunks = tmp.getAdjacentChunks(rstart, noff);
				int t = 0;
				
				for( DataChunk s: scunks )
				{
					t +=s.getSize();
				}
					
				if( t < dataSize )
				{
					take = i;
					dataSize = t;
				}
				
			}
			count[take]++;
			if( this.doRead ){
			Host h = ci.getReplicateHost(new Partition(array.getZid(),array.getId(),p,new RID(0)), new RID( take ));
		
		
			OptimusDataProtocol dp = h.getDataProtocol();
			// start in the overall array
			System.out.println(c);
			System.out.println(Arrays.toString(rstart));
			System.out.println(Arrays.toString(noff));
			double [] data = dp.readDouble(array.getId(),p,new OptimusShape(c.getChunkSize()), new OptimusShape(rstart), new OptimusShape(noff)).getData();
			OptimusDataManager.readFromMem(nstart, noff, noff, off, data, nstart, rdata, start);
			}
			totalSize += dataSize;
		}
		this.time = System.currentTimeMillis() - b;
		for(int i = 0 ; i < count.length; i++)
		{
			System.out.print(count[i]+" ");
		}
		System.out.println();
		return rdata;
	}

	public void CostEstimate(int []vshape,int []pshape,Vector<int[]> shapes, int []start, int []off) throws IOException, WrongArgumentException
	{
		
		DataChunk chunk = new DataChunk(vshape,pshape);
		Set<DataChunk> chunks = chunk.getAdjacentChunks(start, off);
		int rsize = 1;
		for(int i = 0 ; i < start.length; i++)
			rsize *= off[i];
		
		if(this.doRead ) {
		}
	
		
		Vector<int []> strategy = shapes;
		int [] count = new int [strategy.size() - 1];
		for(DataChunk c:chunks)
		{
			int dataSize = java.lang.Integer.MAX_VALUE;
			int take = -1;
		
			int [] nstart = new int [start.length];
			int [] noff = new int [start.length];
			// start in the partition
			int [] rstart = new int [start.length];
		
			
			int [] cstart = c.getStart();
			int [] coff = c.getChunkStep();
			
			for(int i = 0 ; i < start.length; i++)
			{
				nstart[i] = start[i] > cstart[i] ? start[i] : cstart[i];
				noff[i] = start[i] + off[i] < cstart[i] + coff[i] ? start[i] + off[i]:cstart[i] + coff[i]; 
				noff[i] -= nstart[i];
				
				rstart[i] =nstart[i] - cstart[i]; // 
			}
			
			for(int i = 0; i < strategy.size() - 1; i++)
			{
				DataChunk tmp = new DataChunk(pshape,strategy.get(i));
				tmp.setStart(c.getStart());
				
				Set<DataChunk> scunks = tmp.getAdjacentChunks(rstart, noff);
				int t = 0;
				
				t=scunks.size();
				/*for( DataChunk s: scunks )
				{
					t +=s.getSize();
				}*/
					
				if( t < dataSize )
				{
					take = i;
					dataSize = t;
				}
				
			}
			count[take]++;
			totalSize += dataSize;
		}
		for(int i = 0 ; i < count.length; i++)
		{
			System.out.print(count[i]+" ");
		}
		System.out.print("\n");
		this.printMatrix();
		System.out.println();
	}

	static public int readFromMem(int start [] , int [] off,int []fsize,int []tsize, 
			double [] fdata,int [] fstart, double[] tdata, int []tstart)
	{
		int size = 1;
		int fpos = 0;
		int tpos = 0 ;
		int [] fjump = new int [start.length];
		int [] djump = new int [start.length];
		for( int i =start.length - 1 ;  i >= 0 ; --i )
		{
			size *= off[i];
			fpos = (fpos == 0 ) ? start[i] -fstart[i] : fpos * fsize[i+1] + start[i] - fstart[i];
			tpos = (tpos == 0 ) ? start[i] - tstart[i] : tpos * tsize[i] + start[i] - tstart[i];
			
		}
		fjump[0] = fsize[0];
		djump[0] = tsize[0];
		for(int i = 1; i < start.length; i++)
		{
			fjump[i] = fsize[i] * fjump[i - 1];
			djump[i] = tsize[i] * djump[i - 1];
		}
		if(size == 0)
		{
			return 0;
		}
		
		int len = start.length - 1;
		int [] iter = new int [len + 1];
		
		int j =  0;
		while(iter[len] < off[len])
		{
			for(int i = 0 ; i < off[0]; i++ )
			{
				tdata[tpos+i] = fdata[fpos+i];
			}
			j = 1;
			while( j <= len )
			{
				iter[j]++;
				fpos += fjump[j - 1];
				tpos += djump[j - 1];
				if(iter[j] < off[j])
				{
					break;
				}else if(j == len){
					break;
				}else{
					
					fpos -= iter[j] * fjump[j - 1];
					tpos -= iter[j] * djump[j - 1];
					
					iter[j] = 0 ;
				}
				j++;
			}
		}
		return size;
	}
	
	
	public void printMatrix()
	{
		System.out.println("TotalSize : Time " + totalSize + ":" + time );
	}

	public boolean isDoRead() {
		return doRead;
	}

	public void setDoRead(boolean doRead) {
		this.doRead = doRead;
	}
}
