package TRANS.test;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.jdom2.JDOMException;

import TRANS.Array.OptimusArray;
import TRANS.Array.OptimusShape;
import TRANS.Array.OptimusZone;
import TRANS.Array.PID;
import TRANS.Array.Partition;
import TRANS.Array.RID;
import TRANS.Client.ZoneClient;
import TRANS.Client.Reader.PartitionReader;
import TRANS.Exceptions.WrongArgumentException;
import TRANS.MR.Median.StrideResultArrayWritable;
import TRANS.Protocol.OptimusCatalogProtocol;
import TRANS.Protocol.OptimusDataProtocol;
import TRANS.util.Host;
import TRANS.util.OptimusConfiguration;

public class ReadStride {
	public static void main(String[] args) throws WrongArgumentException, JDOMException, IOException {
		OptimusConfiguration conf = new OptimusConfiguration("./conf");
		PartitionReader reader = new PartitionReader(conf);
		String zoneName = TestConst.testZoneName;
		String arrayName = TestConst.testArrayName;
		int[] start = { 0, 0 };
		int[] off = { 4, 1 };
		ZoneClient zclient = new ZoneClient(conf);
		OptimusZone zone = zclient.openZone(zoneName);
		if (zone == null) {
			System.out.print("UnCreated zone or unknown error happened");
		}
		OptimusCatalogProtocol ci = zclient.getCi();
		OptimusArray array = ci.openArray(zone.getId(), new Text(arrayName));
		Partition p = new Partition(zone.getId(),array.getId(),new PID(3),new RID(0));
		Host h = ci.getReplicateHost(p, new RID(0));
		OptimusDataProtocol dp = h.getDataProtocol();
		
		StrideResultArrayWritable a = dp.readStride(p, new OptimusShape(TestConst.psize), new OptimusShape(TestConst.srcStart),
				new OptimusShape(TestConst.vsize),new OptimusShape( TestConst.stride));
	
		System.out.print(Arrays.toString(a.getResult()));
	}
}
