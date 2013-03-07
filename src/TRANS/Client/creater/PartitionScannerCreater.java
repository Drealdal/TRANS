package TRANS.Client.creater;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Vector;

import TRANS.OptimusReplicationManager;
import TRANS.Array.ArrayID;
import TRANS.Array.DataChunk;
import TRANS.Array.OptimusShape;
import TRANS.Array.OptimusZone;
import TRANS.Array.PID;
import TRANS.Array.Partition;
import TRANS.Array.RID;
import TRANS.Client.ArrayCreater;
import TRANS.Protocol.OptimusCatalogProtocol;
import TRANS.util.ByteWriter;
import TRANS.util.Host;
import TRANS.util.OptimusConfiguration;
import TRANS.util.OptimusDouble2ByteStreamWriter;

public class PartitionScannerCreater extends PartitionCreater implements Runnable {
	
	private ArrayCreater acreater = null;
	private OptimusCatalogProtocol ci = null;
	private ArrayID arrayId;
	OptimusScanner scanner = null;
	private OptimusZone zone = null;
	private PID partitionId;
	OptimusConfiguration conf;
	private double []data;
	private DataChunk chunk = null;
	private String vname = null;
	private long scanTime = 0; 
	private long writeTime = 0;
	private long waitTime = 0;
	
	private int[] srcShape = null;
	DataOutputStream cout = null;
	DataInputStream cin = null; 
	
	public PartitionScannerCreater(OptimusConfiguration conf,ArrayCreater acreater, OptimusCatalogProtocol ci,OptimusScanner scanner, OptimusZone zone,
			ArrayID arrayid, PID partitionId,DataChunk chunk,String vname,int [] srcShape) throws UnknownHostException, IOException
	{

		this.acreater = acreater;
		this.scanner = scanner;
		this.arrayId = arrayid;
		this.partitionId = partitionId;
		this.zone = zone;
		this.ci = ci;
		this.chunk = chunk;
		this.conf = conf;
		this.vname = vname;
		this.srcShape = srcShape;
	}
	
	@Override
	public void run() {
		
		synchronized(scanner){
			long btime = System.currentTimeMillis();
			this.data = scanner.readChunkDouble(chunk, vname);
			long etime = System.currentTimeMillis();
			this.scanTime = etime - btime;
		}	
		try {
			
		
			long btime = System.currentTimeMillis();
			OptimusDouble2ByteStreamWriter writer = (OptimusDouble2ByteStreamWriter) this.getWriter();
			if( data == null )
			{
				System.out.println("data null");
			}
			for(int i = 0 ; i < data.length; i++)
			{
				writer.writeDouble(data[i]);
			//	cout.writeDouble(data[i]);
			}
			writer.close();
			
		//	byte [] tbyte = new byte[data.length * 4];
		//	cout.write(tbyte);
		//	writer.writeFloat(data);
		//	writer.close();
			long etime = System.currentTimeMillis();
			this.writeTime = etime - btime;
			
			this.close();
			
		} catch (Exception e) {

			e.printStackTrace();
		}
		System.out.println("Scann: Write: Wait " + this.scanTime+":" + this.writeTime +":" + this.waitTime);
		this.acreater.AddTask();

	}
	
	
	
	/**
	 * @param args
	 * @throws IOException 
	 * @throws UnknownHostException 
	 */
	public static void main(String[] args) throws UnknownHostException, IOException {
		
		int []dstshape = {2,2,2};
	 	int []dstshape2 = {1,4,4};
	 	int []dstshape3 = { 4,4,2};
	 	
		Vector<int []> shapes = new Vector<int []>();
		shapes.add(dstshape);
	
		shapes.add(dstshape2);
		shapes.add(dstshape3);
		
		float []data = new float [64];
		for(int i = 0 ; i < 64 ; i ++)
			data[i] = i;
		for(int i = 0 ; i < 2; i++)
		{
			//PartitionCreater pc = new PartitionCreater(0,i,vsize,start,shapes,data,srcstep);
			
			
		//	pc.run();
			
		}
	}

	@Override
	public ByteWriter getWriter() {
		int relicateSize = zone.getStrategy().getShapes().size() - 1;
		Host host = ci.getReplicateHost(new Partition(zone.getId(),this.arrayId,this.partitionId,new RID(0)),new RID(relicateSize - 1));
		
		
		OptimusDouble2ByteStreamWriter writer = null;
		try {
			host.ConnectReplicate();
			cout = host.getReplicateWriter();
			cin = host.getReplicateReply();
			
			/*
			 * shapes �����һ���Ǵ�����ʱ���client���͵�����ʽ
			 */
			cout.writeInt( relicateSize );
			OptimusReplicationManager rmanager = new OptimusReplicationManager(conf,ci);
			Partition p = new Partition(rmanager,zone.getId(),this.arrayId,this.partitionId,new RID(relicateSize - 1));
			p.write(cout);
			
			new OptimusShape(this.srcShape).write(cout);
			
			writer = new OptimusDouble2ByteStreamWriter(1024*1024,cout);
			
			
		} catch (Exception e) {

			e.printStackTrace();
			return null;
		}
		return writer;
			
	}

	@Override
	public boolean close() {
		boolean succ = false;
		try{

		long etime = System.currentTimeMillis();
		if( OptimusReplicationManager.REPLICATE_OK == cin.readInt() )
		{
			succ = true;
		}
		this.waitTime = System.currentTimeMillis();
		this.waitTime -= etime;
		cout.close();
		cin.close();
		}catch(IOException e)
		{
			e.printStackTrace();
		}
		return succ;
	}

}
