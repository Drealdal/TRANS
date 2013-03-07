package TRANS.Protocol;

import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.ipc.VersionedProtocol;

import TRANS.Array.ArrayID;
import TRANS.Array.OptimusShape;
import TRANS.Array.PID;
import TRANS.Array.Partition;
import TRANS.Calculator.OptimusCalculator;
import TRANS.Data.Optimus1Ddata;

public interface OptimusCalculatorProtocol extends VersionedProtocol {
	static public long versionID = 1; 
	public Optimus1Ddata FindMaxMin(ArrayID aid, PID pid, OptimusShape start, OptimusShape off)throws Exception;
	public BooleanWritable JoinArray(Partition array1, Partition array2,Partition array3, OptimusCalculator c)throws IOException;
}
