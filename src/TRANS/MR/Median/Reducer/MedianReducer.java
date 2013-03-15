package TRANS.MR.Median.Reducer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import TRANS.MR.Median.StrideResult;

public class MedianReducer extends 	Reducer<IntWritable, StrideResult, IntWritable, DoubleWritable> {
	
	private int len = 1;
	private int []stride = null;
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		JobConf conf = (JobConf)context.getConfiguration();
		String s = conf.get("TRANS.range.stride");
		String []st = s.split(",");
		stride = new int[st.length];
		
		for(int i = 0; i < st.length; i++)
		{
			stride[i] = Integer.parseInt(st[i]);
			len *=stride[i];
		}
		
		super.setup(context);
	}

	public void reduce(IntWritable key, Iterable<StrideResult> values,
			Context context) throws InterruptedException, IOException {
		
		Iterator<StrideResult> it = values.iterator();
		StrideResult r = it.next();
		while(it.hasNext())
		{
			r.add(it.next());
		}
		double []data = r.getData();
		Arrays.sort(data);
		context.write(key, new DoubleWritable(data[data.length/2]));
		
	}
}
