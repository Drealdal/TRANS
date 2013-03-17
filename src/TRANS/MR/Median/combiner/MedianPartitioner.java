package TRANS.MR.Median.combiner;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Partitioner;

import TRANS.Array.DataChunk;
import TRANS.MR.Median.StripeMedianResult;

public class MedianPartitioner extends Partitioner<IntWritable, StripeMedianResult> implements Configurable {
	private DataChunk localChunk = null;
	@Override
	public int getPartition(IntWritable key, StripeMedianResult value,
			int numPartitions) {
		this.localChunk.getChunkByOff(key.get());
		return this.localChunk.getChunkNum()%numPartitions;
	}

	@Override
	public void setConf(Configuration job) {
		// TODO Auto-generated method stub
		String s = job.get("TRANS.range.stride");
		String roff = job.get("TRANS.range.offset");
		String pshape = job.get("TRANS.output.array.pshape");
		String []st = s.split(",");
		String []pstr = pshape.split(",");
		
		String []offs = roff.split(",");
		
		int[] stride = new int[st.length];
		int[] ps = new int[st.length];
		int[] rangeOff = new int[st.length];
		
		int[] resultShape = new int [st.length];
		for(int i = 0; i < st.length; i++)
		{
			stride[i] = Integer.parseInt(st[i]);
			rangeOff[i]=Integer.parseInt(offs[i]);
			resultShape[i] = rangeOff[i]/stride[i];	
			ps[i]=Integer.parseInt(pstr[i]);
		}
		this.localChunk = new DataChunk(resultShape,ps);
	}

	@Override
	public Configuration getConf() {
		// TODO Auto-generated method stub
		return null;
	}

}
