package TRANS.MR.Median.combiner;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import TRANS.MR.Median.StripeMedianResult;


public class MedianCombiner extends Reducer<IntWritable, StripeMedianResult,IntWritable, StripeMedianResult> {
	public void reduce(IntWritable key, Iterable<StripeMedianResult> values,
			Context context) throws InterruptedException, IOException {
		Iterator<StripeMedianResult> it = values.iterator();
		StripeMedianResult result = it.next();
		if(result.isFull())
		{
			context.write(key, result);
		}else{
			while(it.hasNext())
			{
				result.add(it.next());
			}
			context.write(key, result);
		}
	}
}
