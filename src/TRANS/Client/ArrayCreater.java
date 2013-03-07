package TRANS.Client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;

import TRANS.Array.ArrayID;
import TRANS.Array.DataChunk;
import TRANS.Array.OptimusZone;
import TRANS.Array.PID;
import TRANS.Client.creater.OptimusScanner;
import TRANS.Client.creater.PartitionScannerCreater;
import TRANS.Exceptions.WrongArgumentException;
import TRANS.Protocol.OptimusCatalogProtocol;
import TRANS.util.OptimusConfiguration;
import TRANS.util.OptimusDefault;

/**
 * @author foryee
 *
 */
public class ArrayCreater {

	private ArrayID id = null;
	private PID cur = null;
	OptimusCatalogProtocol ci = null;
	private String name = null;
	private OptimusZone zone = null;
	private ExecutorService exe = null;
	private float defaultValue = 0;
	private Semaphore semp= null;
	OptimusConfiguration conf;
	private int []srcShape = null;
	
	/**
	 * @param conf
	 * @param zone
	 * @param name
	 * @param tsize: thread number used to create array
	 * @throws IOException
	 */
	public ArrayCreater(OptimusConfiguration conf, OptimusZone zone,int [] srcShape,String name,int tsize,float devalue) throws IOException
	{
		String catalogHost = conf.getString("Optimus.catalog.host", OptimusDefault.CATALOG_HOST);
		int catalogPort = conf.getInt("Optimus.catalog.port", OptimusDefault.CATALOG_PORT);
	
		this.ci = (OptimusCatalogProtocol) RPC.waitForProxy(OptimusCatalogProtocol.class,
				OptimusCatalogProtocol.versionID,
				new InetSocketAddress(catalogHost,catalogPort), new Configuration());
		this.name = name;
		this.zone = zone;
		this.conf = conf;
		this.defaultValue = devalue;
		this.semp = new Semaphore(tsize*2);
		this.exe = Executors.newFixedThreadPool(tsize);
		this.srcShape = srcShape;
		//new DataChunk(zone.getSize().getShape(),zone.getPstep().getShape());
	}
	
	public void create() throws WrongArgumentException
	{
//		this.ci.createArray(name, vsize, chunkStep, chunkStrategy)
	
		this.id = this.ci.createArray(zone.getId(),new Text(name),new FloatWritable(defaultValue));
		this.cur = new PID(0);
	}
	public void RemoveTask() throws InterruptedException
	{
		this.semp.acquire();
	}
	public void AddTask()
	{
		this.semp.release();
	}
	public void createPartition(OptimusScanner scanner, DataChunk chunk,String vname) throws UnknownHostException, IOException, InterruptedException
	{	
		
		DataChunk c = new DataChunk(chunk);
		PartitionScannerCreater pc = new PartitionScannerCreater(conf,this,ci,scanner,zone,this.id,this.cur,c,vname,this.srcShape);
		this.exe.execute(pc);
		this.RemoveTask();
		this.nextPartition();
		
	}
	
	private void nextPartition()
	{
		this.cur = new PID(this.cur.getId()+1);
	}
	public ArrayID getId() {
		return id;
	}

	public void setId(ArrayID id) {
		this.id = id;
	}

	public PID getCur() {
		return cur;
	}

	public void setCur(PID cur) {
		this.cur = cur;
	}
	
	public boolean close(long timeout, TimeUnit unit) throws InterruptedException
	{
		this.exe.shutdown();
		return this.exe.awaitTermination(timeout, unit);
	}
	
}
