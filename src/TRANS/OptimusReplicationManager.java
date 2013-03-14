package TRANS;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import TRANS.Array.ArrayID;
import TRANS.Array.OptimusZone;
import TRANS.Array.PID;
import TRANS.Array.Partition;
import TRANS.Array.RID;
import TRANS.Array.ZoneID;
import TRANS.Exceptions.WrongArgumentException;
import TRANS.Protocol.OptimusCatalogProtocol;
import TRANS.util.Host;
import TRANS.util.OptimusConfiguration;
import TRANS.util.OptimusDefault;

public class OptimusReplicationManager extends Thread  {

	private OptimusCatalogProtocol ci = null;
	
	static  public int REPLICATE_OK = 1;
	static public int REPLICATE_FAILURE = -1;

	private String hostname = null;
	private OptimusInstanceID instanceId = new OptimusInstanceID(-1);
	private int replicatePort = 0;
	private int dataPort = 0;
	private OptimusConfiguration conf = null;
	
	ServerSocket socket = null;
	ConcurrentHashMap<ArrayID,ConcurrentHashMap<PID,Partition>> partitions = new ConcurrentHashMap<ArrayID,ConcurrentHashMap<PID,Partition>>();
	ConcurrentHashMap<ZoneID,OptimusZone> zones = new ConcurrentHashMap<ZoneID,OptimusZone>();
	
	LocalDataManager dataManager = null;
	public OptimusReplicationManager(OptimusConfiguration conf, OptimusCatalogProtocol ci) throws IOException, WrongArgumentException {
		
		hostname = InetAddress.getLocalHost().getHostName();
		
		this.replicatePort = conf.getInt("Optimus.rmanager.port",OptimusDefault.REPLICATE_PORT);
		this.dataPort = conf.getInt("Optimus.data.port", OptimusDefault.DATA_PORT);
		
		this.ci = ci;
		this.conf = conf;	
		dataManager = new LocalDataManager(conf.getString("Optimus.data.path", OptimusDefault.DATA_PATH));
	
	}
	public OptimusCatalogProtocol getCI()
	{
		return this.ci;
	}
	public OptimusZone getZone(ZoneID id)
	{
		if(this.zones.containsKey(id))
		{
			return this.zones.get(id);
		}else{
			try{
				return ci.openZone(id);
						
			}catch(Exception e)
			{
				return null;
			}
		}
	}

	public Partition getPartitionById(ArrayID aid,PID pid)
	{
		ConcurrentHashMap<PID,Partition> p = this.partitions.get(aid);
		if( p == null ){
			return null;
		}
		return p.get(pid);
	}
	public void putPartition(Partition p)
	{
		ConcurrentHashMap<PID,Partition> ps = this.partitions.get(p.getArrayid());
		if(ps == null)
		{
			ps = new ConcurrentHashMap<PID,Partition>();
			this.partitions.put(p.getArrayid(), ps);
		}
		ps.put(p.getPid(), p);
	}
	/**
	 * TODO need to move to OptimusNode
	 */
	private void register()
	{
		Host h = new Host(hostname,replicatePort,this.dataPort,instanceId);
		System.out.println("Before sending:" + h.toString());
		this.instanceId = ci.Register(h);
	}

	 public Host getReplicateHost(Partition p, int d)
			throws UnknownHostException, IOException {
		 Host h = ci.getReplicateHost(p, new RID(d));
		 
		return h;
		 //return ci.getReplicateHost(new IntWritable(p.getPid()), new IntWritable(d));
	}

	 public void openPatitionDataFile(Partition p) throws IOException
	 {
		 this.dataManager.createPatition(p);
	 }
	 
	 public void createPatitionDataFile(Partition p) throws IOException
	 {
		 this.dataManager.createPatition(p);
	 }
	/**
	 * @param args
	 * @throws IOException
	 * @throws WrongArgumentException 
	 * @throws UnknownHostException
	 */
	public void server() throws IOException, WrongArgumentException
	 {
		this.register();
		socket = new ServerSocket(this.replicatePort);
		
		Timer t = new Timer();
		t.schedule(new OptimusTimeRunner(this), 0, this.conf.getInt("Optimus.hearbeat.time", OptimusDefault.HEARTBEAT_TIME)*60*1000);
		//ExecutorService  exe =   Executors.newCachedThreadPool();
		ExecutorService exe = Executors.newFixedThreadPool(this.conf.getInt("Optimus.datanode.thread", 3)); 
		while(true)
		 {
			 Socket sin = socket.accept();
			 socket.setReceiveBufferSize(this.conf.getInt("Optimus.socket.buffersize", OptimusDefault.SOCKET_BUFFER_SIZE));
			 DataOutputStream cout = new DataOutputStream(sin.getOutputStream());
			 DataInputStream cin = new DataInputStream(sin.getInputStream());
			 Partition p = new Partition(this,cin,cout,sin);
			 exe.execute(p);
			 
		 }
	 }
	
	@Override
	public void interrupt() {
		
		// TODO Auto-generated method stub
		try {
			this.socket.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		super.interrupt();
	}
	@Override
	public void run() {
		try {
			this.server();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (WrongArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	public void initData() throws WrongArgumentException
	{
		String dataPath = conf.getString("Optimus.data.path", "./data");
	
		File dir = new File(dataPath);

		if (dir == null || !dir.isDirectory()) {
			throw new WrongArgumentException("Data Directory",
					dataPath+"Not directroy");
		}

		File[] fname = dir.listFiles(new FilenameFilter() {
			// ������ʵ�ֽӿ�FilenameFileter��Ψһ����
			public boolean accept(File dir, String name) {
				return name.indexOf(OptimusDefault.DATA_FILE_SUFIX) != -1;
			}
		});
		for (File file : fname) {
			String fn = file.getName();
			fn.substring(0, fn.indexOf(OptimusDefault.DATA_FILE_SUFIX));
			String [] ids = fn.split("_");
			ZoneID zid = new ZoneID(Integer.parseInt(ids[0]));
			ArrayID aid = new ArrayID(Integer.parseInt(ids[1]));
			PID pid = new PID(Integer.parseInt(ids[2]));
			RID rid = new RID(Integer.parseInt(ids[3]));
			
			Partition p = new Partition(this,zid,aid,pid,rid);
			ConcurrentHashMap<PID,Partition> ps = this.partitions.get(p.getArrayid());
			if(ps == null)
			{
				ps = new ConcurrentHashMap<PID,Partition> ();
				this.partitions.put(p.getArrayid(), ps);
			}
			ps.put(p.getPid(), p);
		}
	
	}
	
	

	
	class OptimusTimeRunner  extends TimerTask{
		private OptimusReplicationManager manager = null;
		public OptimusTimeRunner(OptimusReplicationManager manager)
		{
			this.manager = manager;
		}
		
		public void report()
		{
			Host h = new Host(hostname,replicatePort,dataPort,instanceId);
			Vector<Partition> spar = new Vector<Partition>();
			Set<Entry<ArrayID, ConcurrentHashMap<PID, Partition>>>  mset = manager.partitions.entrySet();
			
			for(Entry<ArrayID,ConcurrentHashMap<PID, Partition>> e: mset)
			{
				ConcurrentHashMap<PID,Partition> ps = e.getValue();
				Set<Entry<PID,Partition>> sets = ps.entrySet();
				for(Entry<PID,Partition> p: sets)
				{
					spar.add(p.getValue());
				}
			}
			
			ci.heartBeat(h, new OptimusPartitionStatus(spar));
		}

		@Override
		public void run() {
			report();
			
		}
		
		
	}

	public ConcurrentHashMap<ArrayID, ConcurrentHashMap<PID, Partition>> getPartitions() {
		return partitions;
	}

	public void setPartitions(
			ConcurrentHashMap<ArrayID, ConcurrentHashMap<PID, Partition>> partitions) {
		this.partitions = partitions;
	}

}
