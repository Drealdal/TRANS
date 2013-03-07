package TRANS.Client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;

import TRANS.Array.OptimusShape;
import TRANS.Array.OptimusShapes;
import TRANS.Array.OptimusZone;
import TRANS.Protocol.OptimusCatalogProtocol;
import TRANS.util.OptimusConfiguration;
import TRANS.util.OptimusDefault;

public class ZoneClient {

	OptimusCatalogProtocol ci = null;
	public ZoneClient(){};
	public ZoneClient(OptimusConfiguration conf) throws IOException
	{
		String catalogHost = conf.getString("Optimus.catalog.host", OptimusDefault.CATALOG_HOST);
		int catalogPort = conf.getInt("Optimus.catalog.port", OptimusDefault.CATALOG_PORT);
	
		this.ci = (OptimusCatalogProtocol) RPC.waitForProxy(OptimusCatalogProtocol.class,
				OptimusCatalogProtocol.versionID,
				new InetSocketAddress(catalogHost,catalogPort), new Configuration());
	}
	public OptimusZone createZone(String name,int []size, int []step,Vector<int []>strategy)
	{
		OptimusZone zone= null;
		try{
			zone = ci.createZone(new Text(name), new OptimusShape(size), new OptimusShape(step),new OptimusShapes(strategy));
		}catch(Exception e)
		{
			System.out.println(e.toString());
			return null;
		}
		
		return zone;
	}
	public OptimusZone openZone(String name)
	{
		OptimusZone zone = null;
		try{
			zone = ci.openZone(new Text(name));
		}catch(Exception e)
		{
			System.out.println(e.toString());
			return null;
		}
		return zone;
	}
}
