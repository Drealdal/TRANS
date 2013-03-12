package TRANS.MR.Combiner;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import TRANS.MR.io.AverageResult;

public class AverageCombiner extends
		Reducer<LongWritable, AverageResult, LongWritable, AverageResult> {

	/**
	 * Reduces values for a given key
	 * 
	 * @param key
	 *            the Key for the given values being passed in
	 * @param values
	 *            a List of AverageResult objects to combine
	 * @param context
	 *            the Context object for the currently executing job
	 * @throws IOException
	 */
	public void reduce(LongWritable key, Iterable<AverageResult> values,
			Context context) throws InterruptedException, IOException {

		AverageResult avgResult = new AverageResult();

		for (AverageResult value : values) {
			avgResult.addResult(value);
		}

		context.write(key, avgResult);
	}
}