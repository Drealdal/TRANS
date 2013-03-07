package TRANS;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.rmi.AlreadyBoundException;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.jdom2.JDOMException;

import TRANS.Array.ArrayID;
import TRANS.Array.OptimusArray;
import TRANS.Array.OptimusShape;
import TRANS.Array.OptimusShapes;
import TRANS.Array.OptimusZone;
import TRANS.Array.PID;
import TRANS.Array.Partition;
import TRANS.Array.RID;
import TRANS.Array.ZoneID;
import TRANS.Exceptions.WrongArgumentException;
import TRANS.OptimusReplicationManager.OptimusTimeRunner;
import TRANS.Protocol.OptimusCatalogProtocol;
import TRANS.Protocol.OptimusDataProtocol;
import TRANS.util.Host;
import TRANS.util.OptimusConfiguration;
import TRANS.util.OptimusDefault;

/*
 * Catalog ����ά�������Ԫ�����Ϣ��CataLog ��SqlLite������Ԫ�����Ϣ
 */
public class OptimusCatalog extends Thread implements OptimusCatalogProtocol, Writable  {

	class OptimusTimeRunner extends TimerTask {
		private OptimusCatalog catalog = null;

		public OptimusTimeRunner(OptimusCatalog catalog) {
			this.catalog = catalog;
		}

		public void check() throws IOException {
			Vector<Host> hs = catalog.checkDataNode();
			if (!hs.isEmpty()) {
				catalog.recover(hs);
			}

		}

		@Override
		public void run() {
			try {
				check();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}
	static int ids = 0;
	/**
	 * @param args
	 * @throws AlreadyBoundException
	 * @throws IOException
	 * @throws ParseException
	 * @throws JDOMException
	 * @throws Exception
	 */
	public static void main(String[] args) throws AlreadyBoundException,
			IOException, ParseException, JDOMException, Exception {
		// TODO Auto-generated method stub

		OptimusCatalog cl = null;

		System.out.println("Starting Catalog Node...");
		CommandLineParser parser = new PosixParser();
		Options options = new Options();
		options.addOption("c", true, "Configuration directory");
		CommandLine cmd = parser.parse(options, args);
		String confDir = cmd.getOptionValue("c");

		OptimusConfiguration conf = null;
		if (confDir != null) {
			conf = new OptimusConfiguration(confDir);
		} else {
			conf = new OptimusConfiguration("./conf");
		}
		
		cl = new OptimusCatalog(conf);
		cl.start();
		Runtime.getRuntime().addShutdownHook(new OptimusCatalogClear(cl));
		cl.join();

	}

	static OptimusInstanceID nextHostId() {
		System.out.println("New Id:" + ids);
		return new OptimusInstanceID(ids++);
	}

	private ConcurrentMap<ZoneID, ConcurrentHashMap<String, ArrayID>> arrayName = new ConcurrentHashMap<ZoneID, ConcurrentHashMap<String, ArrayID>>();
	private ConcurrentMap<ArrayID, OptimusArray> arrays = new ConcurrentHashMap<ArrayID, OptimusArray>();
	private OptimusConfiguration conf = null;
	private ConcurrentHashMap<OptimusInstanceID, OptimusPartitionStatus> hostStatus = new ConcurrentHashMap<OptimusInstanceID, OptimusPartitionStatus>();
	DataOutputStream metaOut = null; // 
	private ConcurrentHashMap<OptimusInstanceID, Host> nodes = new ConcurrentHashMap<OptimusInstanceID, Host>();

 	private ConcurrentSkipListSet<Host> liveNodes = new ConcurrentSkipListSet<Host>();
//	private ConcurrentSkipListSet<Host> deadNodes = new ConcurrentSkipListSet<Host>();

	private ConcurrentMap<ArrayID, ConcurrentHashMap<PID, Host []>> partitions = new ConcurrentHashMap<ArrayID, ConcurrentHashMap<PID, Host[]>>();
	private Server server = null;

	private ConcurrentHashMap<String, ZoneID> zoneName = new java.util.concurrent.ConcurrentHashMap<String, ZoneID>();

	private ConcurrentHashMap<ZoneID, OptimusZone> zones = new java.util.concurrent.ConcurrentHashMap<ZoneID, OptimusZone>();

	OptimusCatalog(OptimusConfiguration conf) throws IOException {
		this.conf = conf;
		String name = this.conf.getString("Optimus.meta.path",
				OptimusDefault.META_PATH);
		String filename = this.conf.getString("Optimus.catalog.filename",
				OptimusDefault.META_FILENAME);
		String path = name + "/" + filename;

		try {
			
			DataInputStream in = new DataInputStream(new FileInputStream(path));
			this.readFields(in);
			in.close();
			
		} catch (Exception e) {
			System.out.println("No meta file found");
		}

		server = RPC.getServer(this, conf.getString("Optimus.catalog.host",
				OptimusDefault.CATALOG_HOST), conf.getInt(
				"Optimus.catalog.port", OptimusDefault.CATALOG_PORT),
				new Configuration());
		
		Timer t = new Timer();
		t.schedule(new OptimusTimeRunner(this), 0, this.conf.getInt("Optimus.hearbeat.time", OptimusDefault.HEARTBEAT_TIME)*60*1000);
	
	}

	public Vector<Host> checkDataNode() {
		System.out.println("Chenking Dead node");
		Vector<Host> deadHosts = new Vector<Host>();
		
		long deadTime = this.conf.getInt("Optimus.hearbeat.time",
				OptimusDefault.HEARTBEAT_TIME) * 60 * 1000  * 3;
		
	//	Set<Entry<OptimusInstanceID, Host>> set = this.liveNodes.entrySet();
		for(Host e: this.liveNodes)
		{
			Host h = (Host) e;
			long t = System.currentTimeMillis();
			System.out.println(t+":"+h.getLiveTime()+"="+(t-h.getLiveTime())+"?" + deadTime);
			if (t- h.getLiveTime()  > deadTime){
				
				h.setDead(true);
				deadHosts.add(h);
			}
		}

		return deadHosts;
	}

	public void close() throws IOException {
		System.out.println("Closing catalog");
		String name = this.conf.getString("Optimus.meta.path",
				OptimusDefault.META_PATH);
		String filename = this.conf.getString("Optimus.catalog.filename",
				OptimusDefault.META_FILENAME);
		String path = name + "/" + filename;
		DataOutputStream out = new DataOutputStream(new FileOutputStream(path));
		this.write(out);
		out.close();
	}

	@Override
	public ArrayID createArray(ZoneID zid, Text name,FloatWritable devalue)
			throws TRANS.Exceptions.WrongArgumentException {

		ConcurrentHashMap<String, ArrayID> aName = this.arrayName.get(zid);
		if (aName == null) {
			throw new TRANS.Exceptions.WrongArgumentException("zid",
					"Not found response zone");
		}
		ArrayID aid = aName.get(new String(name.getBytes()));
		if (aid != null) {
			throw new TRANS.Exceptions.WrongArgumentException("name",
					"Existing array for zone:" + zid);
		}

		aid = new ArrayID(this.arrays.size());
		OptimusArray array = new OptimusArray(zid, aid, new String(
				name.getBytes()),devalue.get());

		this.arrays.put(aid, array);
		aName.put(new String(name.getBytes()), aid);
		System.out.print("Created Array:" + name + "id is " + aid.getArrayId()
				+ "\n");

		return aid;
	}

	@Override
	public OptimusZone createZone(Text name, OptimusShape size,
			OptimusShape step, OptimusShapes strategy)
			throws WrongArgumentException {
		ZoneID id = null;
		if ((id = this.zoneName.get(new String(name.getBytes()))) != null) {
			throw new WrongArgumentException("zone name", "Exsiting Zone");
		}
		id = this.nextZoneID();

		OptimusZone zone = new OptimusZone(new String(name.getBytes()), id,
				size, step, strategy);

		this.zoneName.put(new String(name.getBytes()), id);
		this.zones.put(id, zone);
		this.arrayName.put(id, new ConcurrentHashMap<String, ArrayID>());
		return zone;
	}

	//��partition �Ļ��ŵ�node
	private Host getLiveNode(Partition p) {
		// TODO determine which node to recover the new node
		OptimusZone z = this.zones.get(p.getZid());
		int r = z.getStrategy().getShapes().size();
		int l ;
		java.util.Random rd = new java.util.Random();
		while(( l = rd.nextInt()% r) != p.getRid().getId() );
		
		return this.getReplicateHost(p, new RID(l));
	}

	private Host getNewNodeForZone(ZoneID id)
	{
		int l ;
		Host host = null;
		java.util.Random rd = new java.util.Random();
		while(true)
		{
			l = rd.nextInt()%this.liveNodes.size();
	//		host = this.liveNodes.;
			if( ! host.isDead() )
			{
				break;
			}
		}
		
		return host;
	}
	
	@Override
	public long getProtocolVersion(String arg0, long arg1) throws IOException {

		return 1;
	}

	@Override
	public Host getReplicateHost(Partition p, RID rid) {

		int n = p.getPid().getId()+ rid.getId() + p.getZid().getId();
		n %= this.nodes.size();
		return this.nodes.get(new OptimusInstanceID(n));
	}
	

	@Override
	public OptimusInstanceID heartBeat(Host host, OptimusPartitionStatus status) {
		System.out.println("Heart Beat from Node:" + host.toString());
		OptimusInstanceID oid = host.getInstanceId();
		if (oid.getId() < 0) {
			oid = this.Register(host);
		} else {
			this.nodes.get(oid).update();
		}
		
		Host uhost = this.nodes.get(oid);
		while (status.hasNext()) {
			Partition tmp = status.nexPartition();
			ConcurrentHashMap<PID, Host[]> phost = this.partitions
					.get(tmp.getArrayid());
			if (phost == null) {
				phost = new ConcurrentHashMap<PID, Host[]>();
				this.partitions.put(tmp.getArrayid(), phost);
			}
			Host [] h = phost.get(tmp.getPid());
			if (h == null) {
				h = new Host [this.zones.get(tmp.getZid()).getStrategy().getShapes().size()];
				phost.put(tmp.getPid(), h);
			}
		//	h.set(tmp.getRid().getId(), host);
			h[tmp.getRid().getId()] = uhost;

		}
		hostStatus.put(host.getInstanceId(), status);

		return oid;
	}

	@Override
	public void interrupt() {
		try {
			this.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		super.interrupt();
	}

	private ZoneID nextZoneID()

	{
		// TODO
		return new ZoneID(this.zoneName.size());
	}

	@Override
	public OptimusArray openArray(ZoneID zid, Text name)
			throws TRANS.Exceptions.WrongArgumentException {

		ConcurrentHashMap<String, ArrayID> aName = this.arrayName.get(zid);
		if (aName == null) {
			throw new TRANS.Exceptions.WrongArgumentException("zid",
					"Not found response zone");
		}

		ArrayID aid = aName.get(new String(name.getBytes()));
		if (aid == null) {
			throw new TRANS.Exceptions.WrongArgumentException("ArrayName " + name,
					"Wrong array name?");
		}
		OptimusArray array = this.arrays.get(aid);
		if (array == null) {
			throw new TRANS.Exceptions.WrongArgumentException("ArrayName",
					"Unknown internal error");
		}
		return array;
	}

	@Override
	public OptimusZone openZone(Text name) throws WrongArgumentException {

		ZoneID id = this.zoneName.get(new String(name.getBytes()));
		if (id == null) {
			throw new WrongArgumentException("zone name", "Unknown Zone");
		}
		return this.zones.get(id);
	}

	@Override
	public OptimusZone openZone(ZoneID id) throws WrongArgumentException {

		OptimusZone zone = null;
		if ((zone = this.zones.get(id)) == null) {
			throw new WrongArgumentException("Zid", "unknown zone");
		}
		return zone;

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		// arrays
		this.arrays.clear();
		this.arrayName.clear();
		int size = in.readInt();
		for (int i = 0; i < size; i++) {
			OptimusArray a = new OptimusArray();
			a.readFields(in);
			this.arrays.put(a.getId(), a);
			ConcurrentMap<String, ArrayID> znames = this.arrayName.get(a
					.getId());
			if (znames == null) {
				znames = new ConcurrentHashMap<String, ArrayID>();
			}
			znames.put(a.getName(), a.getId());
		}

		// zones
		size = in.readInt();
		for (int i = 0; i < size; i++) {
			OptimusZone zone = new OptimusZone();
			zone.readFields(in);
			this.zones.put(zone.getId(), zone);
			this.zoneName.put(zone.getName(), zone.getId());
		}

	}

	private void recover(Host host) throws IOException {
		System.out.println("Recovying "+host);
		OptimusPartitionStatus status = this.hostStatus.get(host
				.getInstanceId());
		if (status == null)
			return;
		Vector<Partition> ps = status.getPartitions();
		for (Partition p : ps) {
			System.out.println("Recoverying "+p);
			Host dph = this.getNewNodeForZone(p.getZid());
			Host ho = this.getLiveNode(p);
			OptimusDataProtocol dp = dph.getDataProtocol();
			dp.RecoverPartition(p,ho);
		}
	}

	public void recover(Vector<Host> hosts) throws IOException {
		for (Host host : hosts) {
			this.recover(host);
		}
	}

	@Override
	public OptimusInstanceID Register(Host host) {

		System.out.println("Register host:" + host);
		if (host.getInstanceId().getId() >= 0) {
			if (this.nodes.get(host.getInstanceId()).equals(host)) {
				return host.getInstanceId();
			} else {
				return null;
			}
		} else {

			host.setInstanceId(OptimusCatalog.nextHostId());
			this.nodes.put(host.getInstanceId(), host);
		}
		System.out.println(host.getInstanceId());
		return host.getInstanceId();
	}

	public void start() {
		new webServer().start();
		server.start();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		// arrays
		Set<Entry<ArrayID, OptimusArray>> s = this.arrays.entrySet();
		
		out.write(s.size());
		OptimusArray a = null;
		
		for(Entry<ArrayID, OptimusArray> e:s)
		{
			a = (OptimusArray) e.getValue();
			a.write(out);
		}

		// zones
		Set<Entry<ZoneID, OptimusZone>> sz = this.zones.entrySet();
		OptimusZone z = null;
		out.writeInt(sz.size());
		
		for(Entry<ZoneID,OptimusZone> e: sz)
		{
			z = (OptimusZone) e.getValue();
			z.write(out);
		}

	}
	
	class webServer extends Thread{

		@Override
		public void run() {
			
				ServerSocket socket = null;
				try {
					socket = new ServerSocket(conf.getInt("Optimus.catalog.webport", 7700));
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				 	
				while(true)
				 {
					 Socket sin;
					try {
						sin = socket.accept();
						 DataOutputStream cout = new DataOutputStream(sin.getOutputStream());
						 DataInputStream cin = new DataInputStream(sin.getInputStream());
						 
						 cout.write(new String("Hello World!").getBytes());
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
				 }
		}
		
		
		
	}
}
