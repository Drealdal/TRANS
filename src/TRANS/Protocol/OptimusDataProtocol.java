package TRANS.Protocol;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.ipc.VersionedProtocol;

import TRANS.Array.ArrayID;
import TRANS.Array.OptimusShape;
import TRANS.Array.PID;
import TRANS.Array.Partition;
import TRANS.Array.RID;
import TRANS.Data.Optimus1Ddata;
import TRANS.util.Host;
import TRANS.util.OptimusData;

public interface OptimusDataProtocol extends VersionedProtocol {
	/**
	 * @param h: ��Ҫ���ݵ��ĵط�
	 * @param shape�� Ŀ�������chunk��ʽ
	 * @return�� �Ƿ�ɹ�
	 */
	static public long versionID = 1; 
	public IntWritable RecoverReadAll(Host h,Partition p,RID rid);
	public OptimusData readDouble(ArrayID aid,PID pid, OptimusShape pshape, OptimusShape start, OptimusShape off) throws IOException;
	public Optimus1Ddata readAverage(ArrayID aid,PID pid,OptimusShape pshape, OptimusShape start, OptimusShape off) throws IOException;
	public IntWritable RecoverPartition(Partition p,Host host);
}
