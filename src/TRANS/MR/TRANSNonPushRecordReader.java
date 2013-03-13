package TRANS.MR;

import java.io.IOException;
import java.util.Arrays;

import TRANS.Array.OptimusArray;
import TRANS.Array.OptimusZone;
import TRANS.Array.PID;
import TRANS.util.OptimusData;

public class TRANSNonPushRecordReader extends TRANSRecordReader<PID, OptimusData> {

	@Override
	public PID getCurrentKey() throws IOException, InterruptedException {
		return split.getPid();
	}

	@Override
	public OptimusData getCurrentValue() throws IOException, InterruptedException {
		OptimusZone zone = split.getZone();
		OptimusArray array = split.getArray();
		System.out.println(array.getName());
		
		System.out.println(Arrays.toString(split.getStart().getShape()));
		System.out.println(Arrays.toString(split.getOff().getShape()));
		return dp.readDouble(array.getId(), split.getPid(), split.getPshape(), split.getStart(), split.getOff());
	}

}
