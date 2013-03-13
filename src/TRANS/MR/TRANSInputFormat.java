package TRANS.MR;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.jdom2.JDOMException;

import TRANS.Array.DataChunk;
import TRANS.Array.OptimusArray;
import TRANS.Array.OptimusShape;
import TRANS.Array.OptimusZone;
import TRANS.Array.PID;
import TRANS.Array.Partition;
import TRANS.Array.RID;
import TRANS.Client.ZoneClient;
import TRANS.Data.Optimus1Ddata;
import TRANS.Exceptions.WrongArgumentException;
import TRANS.Protocol.OptimusCatalogProtocol;
import TRANS.util.OptimusConfiguration;
import TRANS.util.OptimusData;
import TRANS.util.TransHostList;
import TRANS.MR.*;

public abstract class TRANSInputFormat<K,V> extends FileInputFormat<K, V>{

	
	

	@Override
	public List<InputSplit> getSplits(JobContext arg0) throws IOException {
		
		JobConf conf = (JobConf) arg0.getConfiguration();
		String zname = conf.get("TRANS.zone.name");
		String aname = conf.get("TRANS.array.name");
		String start = conf.get("TRANS.range.start");
		String off = conf.get("TRANS.range.offset");
		String confDir = conf.get("TRANS.conf.dir");
		if(confDir==null)
		{
			confDir = System.getenv("OPTIMUS_CONF");
		}
		System.out.println(zname+":"+aname+start+":"+off);
		String []starts = start.split(",");
		String []offs = off.split(",");
		
		if(starts.length != offs.length)
		{
			System.exit(-1);
		}
		int [] spoint = new int [starts.length];
		int [] opoint = new int [starts.length];
		
		for( int i = 0 ; i < spoint.length; i++ )
		{
			spoint[i] = Integer.parseInt(starts[i]);
			opoint[i] = Integer.parseInt(offs[i]);
		}
		ZoneClient zclient = null;
		try {
			zclient = new ZoneClient(new OptimusConfiguration(confDir));
		} catch (WrongArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(-2);
		} catch (JDOMException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		OptimusZone zone = zclient.openZone(zname);
		if(zone == null)
		{
			System.out.print("UnCreated zone or unknown error happened");
			System.exit(-1);
		}
		DataChunk chunk = new DataChunk(zone.getSize().getShape(),zone.getPstep().getShape());
		Set<DataChunk> chunks = chunk.getAdjacentChunks(spoint, opoint);
		OptimusCatalogProtocol ci = zclient.getCi();
		OptimusArray array = null;
		try {
			array = ci.openArray(zone.getId(),new Text(aname));
		} catch (WrongArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(-1);
		}
		List<InputSplit> splits = new LinkedList<InputSplit>();
		
		System.out.println("Querying:" + zone.getName()+"."+array.getName());
		for(DataChunk c: chunks)
		{
			int [] nstart = new int [spoint.length];
			int [] noff = new int [spoint.length];
			// start in the partition
			int [] rstart = new int [spoint.length];
		
			
			int [] cstart = c.getStart();
			int [] coff = c.getChunkStep();
			
			for(int i = 0 ; i < spoint.length; i++)
			{
				nstart[i] = spoint[i] > cstart[i] ? spoint[i] : cstart[i];
				noff[i] = spoint[i] + opoint[i] < cstart[i] + coff[i] ? spoint[i] + opoint[i]:cstart[i] + coff[i]; 
				noff[i] -= nstart[i];
				rstart[i] =nstart[i] - cstart[i]; // 
			}
			OptimusShape s = new OptimusShape(rstart);
			OptimusShape o = new OptimusShape(noff);
			PID p = new PID(c.getChunkNum());
			Partition pd = new Partition(zone.getId(),array.getId(),p,new RID(0));
			TransHostList l = null;
			try {
				l = ci.getHosts(pd);
			} catch (WrongArgumentException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.exit(-3);
			}
			TRANSInputSplit split = new TRANSInputSplit(zone,array,p,s,o,confDir);
			split.setHosts(l);
			split.setPshape(new OptimusShape(c.getChunkSize()));
			splits.add(split);
			
		}
		return splits;
	}

	

}
