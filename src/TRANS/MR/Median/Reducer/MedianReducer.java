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

public class MedianReducer extends 	Reducer<IntWritable, StrideResult, IntWritable, DoubleWritable> {
	
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

	public void reduce(IntWritable key, Iterable<StrideResult> values,
			Context context) throws InterruptedException, IOException {
		System.out.println("Here i am");
		Iterator<StrideResult> it = values.iterator();
		
		DataChunk partition = new DataChunk(rangeOff, stride);
		int pnum = key.get();
		DataChunk tmpar = null;
		for (int i = 0; i < rangeStart.length; i++) {
			
			while (partition != null && partition.getChunkNum() < pnum) {
				tmpar = partition;
				partition = partition.moveUp(i);
				
			}
			if(partition == null) partition = tmpar;
			if (partition.getChunkNum() == pnum) {
				break;
			}
			partition = tmpar;
			
		}
		
		StrideResult result = new StrideResult(new double[len],partition.getStart(),partition.getChunkSize());
		while(it.hasNext())
		{
			StrideResult tmp = it.next();
			System.out.println(tmp);
			result.add(tmp);
		}
		double []data = result.getData();
		System.out.println(result.toString());
		Arrays.sort(data);
		context.write(key, new DoubleWritable(data[data.length/2]));
		
	}
}
