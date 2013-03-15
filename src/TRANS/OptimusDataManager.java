package TRANS;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Set;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;

import TRANS.Array.ArrayID;
import TRANS.Array.DataChunk;
import TRANS.Array.OptimusShape;
import TRANS.Array.OptimusZone;
import TRANS.Array.PID;
import TRANS.Array.PartitialCreateResult;
import TRANS.Array.Partition;
import TRANS.Array.RID;
import TRANS.Calculator.OptimusCalculator;
import TRANS.Client.creater.PartitionStreamCreater;
import TRANS.Data.Optimus1Ddata;
import TRANS.MR.io.AverageResult;
import TRANS.Protocol.OptimusCalculatorProtocol;
import TRANS.Protocol.OptimusDataProtocol;
import TRANS.util.ByteWriter;
import TRANS.util.Host;
import TRANS.util.OptimusConfiguration;
import TRANS.util.OptimusData;
import TRANS.util.OptimusDouble2ByteRandomWriter;
import TRANS.util.OptimusTranslator;
import TRANS.util.TRANSDataIterator;

/*
 * serve data to the client or the other component of the cluster
 */
public class OptimusDataManager extends Thread implements OptimusDataProtocol,
		OptimusCalculatorProtocol {

	int dataport = 0;

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Thread#start()
	 */
	@Override
	public synchronized void start() {
		this.server.start();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Thread#interrupt()
	 */
	@Override
	public void interrupt() {
		// TODO Auto-generated method stub
		this.server.stop();
		super.interrupt();
	}

	OptimusReplicationManager rmanger = null;//
	private Server server = null;

	java.util.Map<Partition, PartitialCreateResult> partitionInCreate = new java.util.HashMap<Partition, PartitialCreateResult>();

	OptimusDataManager(OptimusConfiguration conf,
			OptimusReplicationManager rmanger, int dataport, String dataDir,
			String metaDir) throws UnknownHostException, IOException {
		this.dataport = dataport;
		server = RPC.getServer(this, InetAddress.getLocalHost()
				.getHostAddress(), dataport, new Configuration());
		this.rmanger = rmanger;

	}

	@Override
	public long getProtocolVersion(String arg0, long arg1) throws IOException {
		// TODO Auto-generated method stub
		return 1;
	}

	@Override
	public IntWritable RecoverReadAll(Host h, Partition p, RID rid) {
		// TODO Auto-generated method stub
		int ret = 0;
		try {
			h.ConnectReplicate();
			DataOutputStream cout = h.getReplicateWriter();
			DataInputStream cin = h.getReplicateReply();

			cout.write(1);// only 1 relication to write
			Partition src = rmanger
					.getPartitionById(p.getArrayid(), p.getPid());
			Partition dst = src.clone();
			dst.setRid(rid);

			dst.writeMeta(cout);
			src.writeMeta(cout);
			int tlen;
			src.open();
			byte[] data = new byte[4096];
			while ((tlen = src.read(data)) > 0) {
				cout.write(data, 0, tlen);
			}
			ret = cin.readInt();

		} catch (IOException e) {
			return new IntWritable(OptimusReplicationManager.REPLICATE_FAILURE);
		}
		return new IntWritable(ret);
	}

	@Override
	public OptimusData readDouble(ArrayID aid, PID pid, OptimusShape pshape,
			OptimusShape starts, OptimusShape offs) throws IOException {

		int[] start = starts.getShape();
		int[] off = offs.getShape();

		Partition p = this.rmanger.getPartitionById(aid, pid);
		if (p == null) {
			System.out.println("Wrong partion");
			return null;
		}
		System.out.println("Reading partition:" + p.toString());

		OptimusZone zone = this.rmanger.getZone(p.getZid());
		Vector<int[]> shapes = zone.getStrategy().getShapes();

		DataChunk chunk = new DataChunk(pshape.getShape(), shapes.get(p
				.getRid().getId()));

		int rsize = 1;
		for (int i = 0; i < off.length; i++)
			rsize *= off[i];
		Set<DataChunk> chunks = chunk.getAdjacentChunks(start, off);

		double[] rdata = new double[rsize];
		TRANSDataIterator ritr = new TRANSDataIterator(rdata, start, off);

		p.open();
		for (DataChunk c : chunks) {
			System.out.println(c);
			double[] data = p.read(c);
			int[] nstart = new int[start.length];
			int[] noff = new int[start.length];

			int[] cstart = c.getStart();
			int[] coff = c.getChunkSize();
			TRANSDataIterator citr = new TRANSDataIterator(data, cstart, coff);
			ritr.init(cstart, coff);
			citr.init(start, off);
			while (citr.next() && ritr.next()) {
				ritr.set(citr.get());
			}

			for (int i = 0; i < start.length; i++) {
				nstart[i] = start[i] > cstart[i] ? start[i] : cstart[i];
				noff[i] = start[i] + off[i] < cstart[i] + coff[i] ? start[i]
						+ off[i] : cstart[i] + coff[i];
				noff[i] -= nstart[i];
			}
			try {
				readFromMem(nstart, noff, c.getChunkSize(), off, data,
						c.getStart(), rdata, start);
			} catch (Exception e) {
				System.out.print("Exception");

			}

		}

		return new OptimusData(rdata, starts, offs, offs);

	}

	/**
	 * @param start
	 *            : Ҫ����λ�õĿ�ʼλ��
	 * @param off
	 *            �� Ҫ�����ݵ�off
	 * @param fsize
	 *            : src �������С
	 * @param tsize
	 *            : Ŀ���ڴ�������С
	 * @param fdata
	 *            : src�����
	 * @param fstart
	 *            : src �������ʼλ��
	 * @param tdata
	 *            �� Ŀ���ڴ�
	 * @param tstart
	 *            : Ŀ���ڴ��start
	 * @return
	 */
	public static int readFromMem(int start[], int[] off, int[] fsize,
			int[] tsize, double[] fdata, int[] fstart, double[] tdata,
			int[] tstart) {
		int size = 1;
		int fpos = 0;
		int tpos = 0;
		int[] fjump = new int[start.length];
		int[] djump = new int[start.length];
		for (int i = 0; i < start.length; i++) {
			size *= off[i];
			fpos = (fpos == 0) ? start[i] - fstart[i] : fpos * fsize[i]
					+ start[i] - fstart[i];
			tpos = (tpos == 0) ? start[i] - tstart[i] : tpos * tsize[i]
					+ start[i] - tstart[i];

		}
		fjump[start.length - 1] = fsize[start.length - 1];
		djump[start.length - 1] = tsize[start.length - 1];
		for (int i = start.length - 2; i >= 0; i--) {
			fjump[i] = fsize[i] * fjump[i + 1];
			djump[i] = tsize[i] * djump[i + 1];
		}
		if (size == 0) {
			return 0;
		}

		int len = start.length - 1;
		int[] iter = new int[len + 1];

		int j = 0;
		while (iter[0] < off[0]) {
			for (int i = 0; i < off[len]; i++) {
				tdata[tpos + i] = fdata[fpos + i];
			}
			j = len - 1;

			while (j >= 0) {
				iter[j]++;
				fpos += fjump[j + 1];
				tpos += djump[j + 1];
				if (iter[j] < off[j]) {
					break;
				} else if (j == 0) {
					break;
				} else {

					fpos -= iter[j] * fjump[j + 1];
					tpos -= iter[j] * djump[j + 1];

					iter[j] = 0;
				}
				j--;
			}

		}
		return size;
	}

	@Override
	public IntWritable RecoverPartition(Partition p, Host host) {

		return this.RecoverReadAll(host, p, p.getRid());

	}

	@Override
	public Optimus1Ddata FindMaxMin(ArrayID aid, PID pid, OptimusShape pshape,
			OptimusShape start, OptimusShape off) throws Exception {
		// TODO Auto-generated method stub
		OptimusData data = this.readDouble(aid, pid, pshape, start, off);

		if (data == null)
			return null;
		double[] fdata = data.getData();

		double[] rminmax = new double[2];
		rminmax[0] = fdata[0];
		rminmax[1] = fdata[0];
		for (int i = 1; i < fdata.length; i++) {
			if (fdata[i] > rminmax[0]) {
				rminmax[0] = fdata[i];
			} else if (fdata[i] < rminmax[1]) {
				rminmax[1] = fdata[i];
			}
		}
		return new Optimus1Ddata(rminmax);
	}

	@Override
	public BooleanWritable JoinArray(Partition array1, Partition array2,
			Partition array3, OptimusCalculator c) throws IOException {

		OptimusZone zone1 = this.rmanger.getZone(array1.getZid());
		OptimusZone zone2 = this.rmanger.getZone(array2.getZid());
		OptimusZone zone3 = this.rmanger.getZone(array3.getZid());

		int[] s1 = zone1.getStrategy().getShapes().get(array1.getRid().getId());
		int[] s2 = zone2.getStrategy().getShapes().get(array1.getRid().getId());
		boolean sameStrategy = true;
		for (int i = 0; i < s1.length; i++) {
			if (s1[i] != s2[i]) {
				sameStrategy = false;
			}
		}
		array1.setRmanager(this.rmanger);
		array2.setRmanager(this.rmanger);
		array3.setRmanager(this.rmanger);

		if (sameStrategy == true) {
			array1.open();
			array2.open();
			DataChunk chunk = new DataChunk(zone1.getPstep().getShape(), s1);
			PartitionStreamCreater creater = new PartitionStreamCreater(
					this.rmanger.getCI(), array3, zone3, chunk.getChunkStep());
			ByteWriter writer = creater.getWriter();

			try {
				do {
					double[] c3 = null;
					double[] c1 = array1.read(chunk);
					if (array1.equals(array2)) {
						c3 = c.calcArray(c1, c1);
					} else {
						double[] c2 = array2.read(chunk);
						c3 = c.calcArray(c1, c2);
					}
					writer.writeDouble(c3);

				} while (chunk.nextChunk());

				return new BooleanWritable(!creater.close());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
		return new BooleanWritable(false);
	}

	@Override
	public AverageResult readAverage(ArrayID aid, PID pid, OptimusShape pshape,
			OptimusShape starts, OptimusShape offs) throws IOException {
		OptimusData data = this.readDouble(aid, pid, pshape, starts, offs);
		double[] d = data.getData();
		double[] rdata = new double[2];
		rdata[0] = d.length;
		AverageResult r = new AverageResult();
		for (int i = 0; i < d.length; i++) {

			r.addValue(d[i]);
		}
		return r;
	}

	@Override
	public BooleanWritable putPartitionData(Partition p, TRANSDataIterator data) throws IOException {
		PartitialCreateResult itp = null;
		DataChunk chunk = null;
		OptimusZone zone = null;
		int size = 0;
		int []shape = null;
		int id = p.getRid().getId();
		synchronized (partitionInCreate) {

			if (this.partitionInCreate.containsKey(p)) {
				itp = this.partitionInCreate.get(p);
			} else {
				zone = this.rmanger.getZone(p.getZid());
				int[] asize = zone.getSize().getShape();
				int[] pstep = zone.getPstep().getShape();

				chunk = new DataChunk(asize, pstep);
				int pnum = p.getPid().getId();
				for (int i = 0; i < asize.length; i++) {
					while (chunk.getChunkNum() < pnum) {
						chunk = chunk.moveUp(i);
					}
					if (chunk.getChunkNum() == pnum) {
						break;
					} else {
						chunk.moveDown(i);
					}
				}
				size = chunk.getSize();
				itp = new PartitialCreateResult(new double[size],
						chunk.getStart(), chunk.getChunkSize());
				this.partitionInCreate.put(p, itp);
			}
		}
		if(!itp.AddResult(data))
		{
			return new BooleanWritable(false);
		}
		itp.init(data.getStart(), data.getShape());
		data.init(itp.getStart(), itp.getShape());
		while(itp.next()&&data.next())
		{
			itp.add(data.get());
		}
		boolean isFull = false;
		synchronized(partitionInCreate){
			itp = partitionInCreate.get(p);
			if(itp.isFull())
			{
				partitionInCreate.remove(p);
				this.rmanger.createPatitionDataFile(p);
				isFull = true;
			}
		}
		if(isFull){
			ByteWriter  w = new OptimusDouble2ByteRandomWriter(1024*1024,p.getDataf(),p);
			shape = zone.getStrategy().getShapes().get(id);
			DataChunk dstChunk = new DataChunk(chunk.getChunkSize(),shape);
			DataChunk srcChunk = new DataChunk(chunk.getChunkSize(),chunk.getChunkSize());
			
			OptimusTranslator trans = new OptimusTranslator(size, srcChunk,dstChunk, w);
			trans.start();
			trans.write(itp.getData());
			this.rmanger.addPartition(p);
		}
		
		if(id > 0){
			Host h = this.rmanger.getReplicateHost(p, id - 1);
			OptimusDataProtocol dp = h.getDataProtocol();
			Partition np = p.clone();
			np.setRid(new RID(id-1));
			return dp.putPartitionData(np, data);
		}
		return new BooleanWritable(true);
		
	}

}
