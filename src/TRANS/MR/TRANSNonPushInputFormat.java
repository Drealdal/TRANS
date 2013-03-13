package TRANS.MR;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import TRANS.Array.PID;
import TRANS.util.OptimusData;

public class TRANSNonPushInputFormat extends TRANSInputFormat<PID,OptimusData> {

	@Override
	public RecordReader<PID, OptimusData> createRecordReader(InputSplit arg0,
			TaskAttemptContext arg1) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return new TRANSNonPushRecordReader();
	}

}
