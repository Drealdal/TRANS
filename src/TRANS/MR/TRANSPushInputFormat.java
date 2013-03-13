package TRANS.MR;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import TRANS.Array.PID;
import TRANS.MR.io.AverageResult;

public class TRANSPushInputFormat extends TRANSInputFormat<PID, AverageResult> {

	@Override
	public RecordReader<PID, AverageResult> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new TRANSPushRecordReader();
	}

}
