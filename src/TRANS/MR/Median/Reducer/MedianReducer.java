package TRANS.MR.Median.Reducer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import TRANS.Array.DataChunk;
import TRANS.MR.Median.StrideResult;
import TRANS.MR.Median.StripeMedianResult;

public class MedianReducer extends 	Reducer<IntWritable, StripeMedianResult, IntWritable, DoubleWritable> {
	
	private int len = 1;
	private int []stride = null;
	private int []rangeStart = null;
	private int []rangeOff = null;
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		JobConf conf = (JobConf)context.getConfiguration();
		String s = conf.get("TRANS.range.stride");
		String rstart = conf.get("TRANS.range.start");
		String roff = conf.get("TRANS.range.offset");

		
		String []st = s.split(",");
		String []starts = rstart.split(",");
		String []offs = roff.split(",");
		
		stride = new int[st.length];
		rangeStart = new int[st.length];
		rangeOff = new int[st.length];
		
		
		for(int i = 0; i < st.length; i++)
		{
			stride[i] = Integer.parseInt(st[i]);
			rangeStart[i]=Integer.parseInt(starts[i]);
			rangeOff[i]=Integer.parseInt(offs[i]);
			len *=stride[i];
		}
		
		super.setup(context);
	}

	public void reduce(IntWritable key, Iterable<StripeMedianResult> values,
			Context context) throws InterruptedException, IOException {
		Iterator<StripeMedianResult> it = values.iterator();
		StripeMedianResult result = it.next();
		if(result.isFull())
		{
			context.write(key, new DoubleWritable(result.getResult()));
		}else{
			while(it.hasNext())
			{
				result.add(it.next());
			}
		}
	}
}
