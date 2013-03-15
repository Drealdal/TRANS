package TRANS.Client.creater;

import java.io.IOException;

import TRANS.Array.Partition;
import TRANS.Protocol.OptimusDataProtocol;
import TRANS.util.Host;
import TRANS.util.TRANSDataIterator;

public class PartitialUploader {
	
	static public boolean CreatePartitial(Host h, Partition p,TRANSDataIterator data) throws IOException{
		OptimusDataProtocol dp = h.getDataProtocol();
		return dp.putPartitionData(p, data).get();
	}
	

}
