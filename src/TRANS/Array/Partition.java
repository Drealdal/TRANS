package TRANS.Array;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.Socket;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.hadoop.io.Writable;


import TRANS.OptimusNode;
import TRANS.OptimusReplicationManager;
import TRANS.Exceptions.WrongArgumentException;
import TRANS.util.Byte2DoubleReader;
import TRANS.util.ByteWriter;
import TRANS.util.Host;
import TRANS.util.OptimusDouble2ByteRandomWriter;
import TRANS.util.OptimusDouble2ByteStreamWriter;
import TRANS.util.OptimusTranslator;
import TRANS.util.OptimusWriter;

public class Partition implements Writable, Runnable {
	static Log log = OptimusNode.LOG;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	private ArrayID arrayid = null;

	DataInputStream cin = null;

	DataOutputStream cout = null;

	RandomAccessFile dataf = null;

	private DataChunk localChunk = null;

	private PID pid = null;// UUID.randomUUID();
	private RID rid = null; // replication number
	private OptimusReplicationManager rmanager = null;
	private int size;
	Socket socket = null;

	private ZoneID zid = null;

	public Partition() {
	}

	public Partition(OptimusReplicationManager rmanager) {
		this.rmanager = rmanager;
	}
	public Partition(OptimusReplicationManager rmanager, DataInputStream cin,
			DataOutputStream cout, Socket socket) throws IOException,
			WrongArgumentException {

		if (rmanager == null) {
			System.out.println("Internal error rmanager null");
		}
		this.rmanager = rmanager;
		this.cin = cin;
		this.cout = cout;
		this.socket = socket;

	}
	public Partition(ZoneID zid, ArrayID aid, PID pid, RID rid)
	{
		this.zid = zid;
		this.arrayid = aid;
		this.pid = pid;
		this.rid = rid;
	}
	public Partition(OptimusReplicationManager rmanager, ZoneID zid,
			ArrayID arrayid, PID partitionid, RID rid) {
		this.zid = zid;
		this.arrayid = arrayid;
		this.pid = partitionid;
		this.rid = rid;

		OptimusZone zone = rmanager.getZone(zid);
		this.localChunk = new DataChunk(zone.getPstep().getShape(), zone
				.getStrategy().getShapes().get(rid.getId()));

		this.rmanager = rmanager;

	}
	public Partition clone() {
		return new Partition(rmanager, this.zid, this.arrayid, this.pid,
				this.rid);
	}

	// ���ص�chunk����

	public void close() throws IOException {
		if (!this.isOpened()) {
			return;
		}
		this.dataf.close();
		this.dataf = null;
	}

	public ArrayID getArrayid() {
		return arrayid;
	}

	public RandomAccessFile getDataf() {
		return dataf;
	}

	public DataChunk getLocalChunk() {
		return localChunk;
	}

	public String getPartitionPath() {
		return this.zid.getId() + "_" + this.arrayid.getArrayId() + "_"
				+ this.pid.getId() + "_" + this.rid.getId() + ".data";
	}

	public PID getPid() {
		return pid;
	}

	public RID getRid() {
		return rid;
	}

	public OptimusReplicationManager getRmanager() {
		return rmanager;
	}

	public int getSize() {
		return size;
	}

	public ZoneID getZid() {
		return zid;
	}

	private boolean isOpened() {
		return (this.dataf != null );
	}

	private void lastReplica(OptimusReplicationManager rmanager,
			DataInputStream cin, DataOutputStream cout) throws IOException, InterruptedException {

		this.readFields(cin);
		
		OptimusShape src = new OptimusShape();
		src.readFields(cin);
		
		this.rmanager = rmanager;
		rmanager.createPatitionDataFile(this);
		OptimusZone zone = rmanager.getZone(this.zid);

		Vector<int[]> shapes = zone.getStrategy().getShapes();
		DataChunk dchunk = new DataChunk(zone.getPstep().getShape(),
				shapes.get(0));
		DataChunk schunk = new DataChunk(zone.getPstep().getShape(),
				src.getShape());

		int[] vsize = zone.getPstep().getShape();
		int len = 1;
		for (int i = 0; i < vsize.length; i++) {
			len *= vsize[i];
		}

		long btime = System.currentTimeMillis();

		if (schunk.ShapeEquals(dchunk)) {
			len *= 8;
			byte[] tmp = new byte[4096];
			while (len > 0) {
				int tlen = cin.read(tmp);
				len -= tlen;
				this.writeData(tmp, 0, tlen);
			}
		} else {
				int tlen = len;
		//	double[] data = new double[tlen];
		/*	double tmp = 0;
			for (int i = 0; i < tlen; i++) {
				schunk.getChunkByOff(i);
				dchunk.getChunkByOff(ChunkTranslater.offTranslate(schunk));
				tmp = cin.readDouble();
				data[dchunk.getOffset()] = tmp;
			}
			for( int i = 0 ; i < data.length; i++)
			{
				this.dataf.writeDouble(data[i]);
			}
			*/
			int fnum = 0;
			Byte2DoubleReader reader = new Byte2DoubleReader(1*1024*1024,null,cin);
			double [] tdouble = null;
			ByteWriter  w = new OptimusDouble2ByteRandomWriter(1024*1024,this.dataf,this);
		//	OptimusWriter writer = new OptimusWriter(w,len*8);
		//	writer.start();
			
			OptimusTranslator trans = new OptimusTranslator(len, schunk, dchunk,w);		
			trans.start();

			while(fnum < len)
			{
				reader.readFromin();
				tdouble = reader.readData();
				if(tdouble == null)
				{
					System.out.println("Unexpected EOF!:fnum"+fnum+" len:"+len);
					continue;
				}
				trans.write(tdouble);
				fnum+=tdouble.length;
				
			/*	for(int i = 0; i < tdouble.length;	 i++)
				{
		
					schunk.getChunkByOff(fnum++);
					dchunk.getChunkByOff(ChunkTranslater.offTranslate(schunk));
					data[dchunk.getOffset()] = tdouble[i];
				}
				*/
			}
		//	writer.write(data);
			
			
		}

		cout.writeInt(OptimusReplicationManager.REPLICATE_OK);
		cin.close();
		cout.close();
		

	}

	private void middleReplica(OptimusReplicationManager rmanager, int rest,
			DataInputStream cin, DataOutputStream cout) throws IOException, InterruptedException {

		this.readFields(cin);
		this.rmanager = rmanager;
		// this.rid.setId(rest);

		OptimusShape src = new OptimusShape();
		src.readFields(cin);
		
		OptimusZone zone = rmanager.getZone(this.zid);
		Vector<int[]> shapes = zone.getStrategy().getShapes();
		DataChunk dchunk = new DataChunk(zone.getPstep().getShape(),
				shapes.get(rest));
		DataChunk schunk = new DataChunk(zone.getPstep().getShape(),
				src.getShape());

		Host h = rmanager.getReplicateHost(this, rest - 1);

		h.ConnectReplicate();
		DataOutputStream nhostOut = h.getReplicateWriter();
		DataInputStream nhostIn = h.getReplicateReply();

		nhostOut.writeInt(rest);

		Partition p = new Partition(this.rmanager, this.zid, this.arrayid,
				this.pid, new RID(this.rid.getId() - 1));
		p.write(nhostOut);
		src.write(nhostOut);
		this.rmanager.createPatitionDataFile(this);

		int[] vsize = zone.getPstep().getShape();
		int len = 1;
		for (int i = 0; i < vsize.length; i++) {
			len *= vsize[i];
		}
		
		
		
		if (schunk.ShapeEquals(dchunk)) {
			len *= 8;
			this.writeMeta(nhostOut);
			byte[] tmp = new byte[4096];
			while (len > 0) {
				int tlen = cin.read(tmp);
				len -= tlen;
				nhostOut.write(tmp, 0, tlen);
				this.writeData(tmp, 0, tlen);
			}
			
		} else {
			
		//	int tlen = len;
			//long System.currentTimeMillis();
			//double [] data = new double [tlen];
			/*double tmp = 0;
			for (int i = 0; i < tlen; i++) {
				schunk.getChunkByOff(i);
				dchunk.getChunkByOff(ChunkTranslater.offTranslate(schunk));
				tmp = cin.readDouble();
				data[dchunk.getOffset()] = tmp;
				data[i] = tmp;
				nhostOut.writeDouble(tmp);

			}
			for( int i = 0 ; i < data.length; i++)
			{
				this.dataf.writeDouble(data[i]);
			}*/
			
			int fnum = 0;
			Byte2DoubleReader reader = new Byte2DoubleReader(1*1024*1024,nhostOut,cin);
			double [] tdouble = null;
			ByteWriter  w = new OptimusDouble2ByteRandomWriter(1024*1024,this.dataf,this);
			OptimusTranslator trans = new OptimusTranslator(len, schunk, dchunk,w);
			//OptimusWriter writer = new OptimusWriter(w,len*8);
			trans.start();
			//writer.start();

			while(fnum < len)
			{
				reader.readFromin();
				tdouble = reader.readData();
				if(tdouble == null)
				{
					System.out.println("Unexpected EOF!:fnum"+fnum+" len:"+len);
					continue;
				}
					trans.write(tdouble);
				fnum += tdouble.length;
				/*
					
				for(int i = 0; i < tdouble.length; i++)
				{
		
					schunk.getChunkByOff(fnum++);
					dchunk.getChunkByOff(ChunkTranslater.offTranslate(schunk));
					data[dchunk.getOffset()] = tdouble[i];
				}
				*/
			}
			//writer.write(data);
			
		}
		long b = System.currentTimeMillis();
		int ret = nhostIn.readInt();
		System.out.println("Waiting time:" + ( System.currentTimeMillis() - b));
		
		cout.writeInt(ret);
		cin.close();
		cout.close();
		nhostIn.close();
		nhostOut.close();

	}

	public void open() throws IOException {
		if (this.isOpened()) {
			return;
		}
		this.rmanager.openPatitionDataFile(this);

	}

	public int read(byte[] buffer) throws IOException {
		return this.dataf.read(buffer);
	}

	public double[] read(DataChunk chunk) throws IOException {
		double[] data = new double[chunk.getSize()];
		dataf.seek(chunk.getStratPos() * 8);
		/*	for (int i = 0; i < chunk.getSize(); i++) {
			data[i] = this.dataf.readDouble();
		}
		*/
		int fnum = 0;
		Byte2DoubleReader reader = new Byte2DoubleReader(1*1024*1024,null,dataf);
		double [] tdouble = null;
		int size = chunk.getSize();
		while(fnum < size)
		{
			reader.readFromin(size - fnum);
			tdouble = reader.readData();
			for(int i = 0; i < tdouble.length; i++)
			{
				data[fnum++] = tdouble[i];
			}
		}
		
		return data;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub

		this.zid = new ZoneID();
		this.zid.readFields(in);
		this.arrayid = new ArrayID();
		this.arrayid.readFields(in);
		this.pid = new PID();
		this.pid.readFields(in);
		this.rid = new RID();
		this.rid.readFields(in);
	}

	public void readMeta(DataInputStream fin) throws IOException {
		if (fin == null) {
			throw new IOException();
		}
		this.zid = new ZoneID();
		this.zid.readFields(fin);

		this.arrayid = new ArrayID();
		arrayid.readFields(fin);

		this.pid = new PID(0);
		this.pid.readFields(fin);

		this.rid = new RID(0);
		this.rid.readFields(fin);

	}

	@Override
	public void run() {
		int rnum;
		try {
			rnum = cin.readInt();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		} // the number of replication to create

		if (rnum == 0) {
			return;
		}
		try {
			if (rnum == 1) {
				lastReplica(rmanager, cin, cout);
			} else {
				middleReplica(rmanager, rnum - 1, cin, cout);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			socket.close();
			///this.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		 ConcurrentHashMap<PID,Partition> ps = this.rmanager.getPartitions().get(this.getArrayid());
		 if(ps == null)
		 {
			 ps = new  ConcurrentHashMap<PID,Partition>();
			 this.rmanager.getPartitions().put(this.getArrayid(), ps);
		 }
		 ps.put(this.getPid(), this);
	}

	public void setArrayid(ArrayID arrayid) {
		this.arrayid = arrayid;
	}

	public void setDataf(RandomAccessFile dataf) {
		this.dataf = dataf;
	}

	public void setLocalChunk(DataChunk localChunk) {
		this.localChunk = localChunk;
	}

	public void setPid(PID pid) {
		this.pid = pid;
	}

	public void setRid(RID rid) {
		this.rid = rid;
	}

	public void setRmanager(OptimusReplicationManager rmanager) {
		this.rmanager = rmanager;
	}

	public void setSize(int size) {
		this.size = size;
	}

	public void setZid(ZoneID zid) {
		this.zid = zid;
	}

	@Override
	public String toString() {
		return "Partition [arrayid=" + arrayid + ", pid=" + pid + ", size="
				+ size + ",rid = " + rid + "]";
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		this.zid.write(out);
		this.arrayid.write(out);
		this.pid.write(out);
		this.rid.write(out);

	}

	public void writeData(byte[] b, int off, int len) throws IOException {
		this.dataf.write(b, off, len);
	}

	public void writeMeta(DataOutputStream fout) throws IOException {
		if (fout == null) {
			throw new IOException();
		}
		zid.write(fout);
		arrayid.write(fout);
		pid.write(fout);
		this.rid.write(fout);

	}

}
