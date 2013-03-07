package TRANS.Protocol;


import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.VersionedProtocol;

import TRANS.OptimusInstanceID;
import TRANS.OptimusPartitionStatus;
import TRANS.Array.ArrayID;
import TRANS.Array.OptimusArray;
import TRANS.Array.OptimusShape;
import TRANS.Array.OptimusShapes;
import TRANS.Array.OptimusZone;
import TRANS.Array.Partition;
import TRANS.Array.RID;
import TRANS.Array.ZoneID;
import TRANS.Exceptions.WrongArgumentException;
import TRANS.util.Host;

public interface OptimusCatalogProtocol extends VersionedProtocol {
	 static public long versionID = 1; 
	 
	 public OptimusInstanceID Register(Host host);
	 public OptimusInstanceID heartBeat(Host host, OptimusPartitionStatus status);

	 public OptimusZone createZone(Text name,OptimusShape size, 
			 OptimusShape step,OptimusShapes strategy)throws WrongArgumentException;
	 public OptimusZone openZone(Text name)throws WrongArgumentException;
	 public OptimusZone openZone(ZoneID id)throws WrongArgumentException;

	 public ArrayID createArray(ZoneID id,Text name,FloatWritable devalue)throws TRANS.Exceptions.WrongArgumentException;
	 public OptimusArray openArray(ZoneID id,Text name)throws TRANS.Exceptions.WrongArgumentException;
	 
	 public Host getReplicateHost(Partition p,RID rid);
}
