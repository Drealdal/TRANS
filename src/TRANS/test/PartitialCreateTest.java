package TRANS.test;

import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.io.Text;
import org.jdom2.JDOMException;

import TRANS.Array.DataChunk;
import TRANS.Array.OptimusArray;
import TRANS.Array.OptimusZone;
import TRANS.Array.PID;
import TRANS.Array.Partition;
import TRANS.Array.RID;
import TRANS.Client.ArrayCreater;
import TRANS.Client.ZoneClient;
import TRANS.Client.creater.OptimusMemScanner;
import TRANS.Client.creater.OptimusScanner;
import TRANS.Exceptions.WrongArgumentException;
import TRANS.Protocol.OptimusCatalogProtocol;
import TRANS.Protocol.OptimusDataProtocol;
import TRANS.util.Host;
import TRANS.util.OptimusConfiguration;
import TRANS.util.TRANSDataIterator;

public class PartitialCreateTest {
 static public void main(String []args) throws WrongArgumentException, JDOMException, IOException
 {
	 int [] srcStart = {0,0};
		int [] vsize = TestConst.srcStart;
		int [] shape = TestConst.psize;
		int [] srcShape = TestConst.sshape;
		int [] dstShape = TestConst.dstShape1;
		int [] dstShape2 = TestConst.dstShape2;
		int [] overlap = TestConst.overlap;
		
		String zoneName = "test";
		String arrayName = "test2";
		Vector<int []>strategy = new Vector<int []>();
		
		strategy.add(dstShape);
		strategy.add(dstShape2);
		strategy.add(srcShape);
		
		OptimusConfiguration conf = new OptimusConfiguration("./conf");
		ZoneClient zcreater = new ZoneClient(conf);
		OptimusZone zone = zcreater.openZone(zoneName);
		if(zone == null)
		{
			zone = zcreater.createZone(zoneName, vsize, shape, strategy);
		}
		if(zone == null)
		{
			System.out.println("Unknown error");
			return;
		}

		ArrayCreater creater = new ArrayCreater(conf,zone,srcShape,arrayName,1,0);
		creater.create();
		
		DataChunk chunk = new DataChunk(vsize,shape);
		int len = 1;
		for(int i = 0; i < vsize.length; i++)
		{
			len *= vsize[i];
		}
		double [] srcData = new double[len];
		for(int i = 0  ; i < srcData.length; i++)
		{
			srcData[i] = i;
		}
		OptimusCatalogProtocol ci = zcreater.getCi();
		OptimusArray array = zcreater.getCi().openArray(zone.getId(), new Text(arrayName));
		OptimusScanner scanner = new OptimusMemScanner(srcData,srcStart,vsize);
		OptimusDataProtocol dp = null;
		do
		{
			double [] data = scanner.readChunkDouble(chunk, "data");
			TRANSDataIterator itr = new TRANSDataIterator(data,chunk.getStart(),chunk.getChunkSize());
			Partition p = new Partition(zone.getId(),array.getId(),
					new PID(chunk.getChunkNum()), new RID(strategy.size() - 2));
			Host h = ci.getReplicateHost(p, p.getRid());
			
			dp = h.getDataProtocol();
			dp.putPartitionData(p, itr);
		}while(chunk.hasNext());
 }
}
