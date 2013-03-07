package TRANS;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.jdom2.JDOMException;

import TRANS.Exceptions.WrongArgumentException;
import TRANS.Protocol.OptimusCatalogProtocol;
import TRANS.util.OptimusConfiguration;
import TRANS.util.OptimusDefault;

public class OptimusNode {

	public static final Log LOG = LogFactory.getLog(OptimusNode.class);
	private OptimusCatalogProtocol ci = null;
	OptimusReplicationManager rmanager = null;
	OptimusDataManager dmanager = null;
	static OptimusNode instance = null;
	OptimusNode(OptimusConfiguration conf) throws UnknownHostException, IOException, WrongArgumentException
	{
		String catahost = conf.getString("Optimus.catalog.host", OptimusDefault.CATALOG_HOST);
		int cataport = conf.getInt("Optimus.catalog.port", OptimusDefault.CATALOG_PORT);
		
		ci = (OptimusCatalogProtocol) RPC.waitForProxy(OptimusCatalogProtocol.class,
				OptimusCatalogProtocol.versionID,
				new InetSocketAddress(catahost,cataport), new Configuration());
		
			int dataPort = conf.getInt("Optimus.data.port", OptimusDefault.DATA_PORT);
		
		String dataDir = conf.getString("Optimus.data.path", OptimusDefault.DATA_PATH);
		String metaDir = conf.getString("Optimus.meta.path",OptimusDefault.META_PATH);
		
		rmanager = new OptimusReplicationManager(conf,ci);
		dmanager = new OptimusDataManager(conf,rmanager,dataPort,metaDir,dataDir);
		
	}
	

	public void stop() throws InterruptedException
	{
		this.rmanager.interrupt();
		this.dmanager.interrupt();
		
	}
	public void start()
	{
		this.rmanager.start();
		this.dmanager.start();
	}
	/**
	 * @param args
	 * @throws WrongArgumentException 
	 * @throws IOException 
	 * @throws UnknownHostException 
	 * @throws InterruptedException 
	 * @throws ParseException 
	 * @throws JDOMException 
	 */
	public static void main(String[] args) throws UnknownHostException, IOException, WrongArgumentException, InterruptedException, ParseException, JDOMException {
		// TODO Auto-generated method stub
		
		CommandLineParser parser = new PosixParser();
		Options options = new Options();
		options.addOption("c", true, "Configuration directory");
		CommandLine cmd = parser.parse(options, args);
		String confDir = cmd.getOptionValue("c");
		
		OptimusNode node = null;
		if(confDir != null )
		{
			node = new OptimusNode(new OptimusConfiguration(confDir));
		}else{
			node = new OptimusNode(new OptimusConfiguration(OptimusDefault.OPTIMUSCONFIG));
		}
		
		node.start();
		node.rmanager.join();
		node.dmanager.join();
	}

}
