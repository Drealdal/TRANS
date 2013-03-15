package TRANS.MR.Median;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Vector;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.jdom2.JDOMException;

import TRANS.Array.OptimusArray;
import TRANS.Array.Partition;
import TRANS.Array.RID;
import TRANS.Client.Reader.PartitionReader;
import TRANS.Exceptions.WrongArgumentException;
import TRANS.MR.TRANSInputSplit;
import TRANS.Protocol.OptimusDataProtocol;
import TRANS.util.*;
public class TRANSMedianRecordReader extends RecordReader<IntWritable,StrideResult>{

	public Map<Integer,StrideResult> values = new HashMap<Integer,StrideResult>();
	//int []start = null;
	private boolean readed = false;
	OptimusDataProtocol dp  = null;
	PartitionReader reader = null;
	StrideResult [] result = null;
	int cur = -1;
	// shape of the partition
	@Override
	public void initialize(InputSplit arg0, TaskAttemptContext context)
			throws IOException, InterruptedException {
		try {
			reader = new PartitionReader(new OptimusConfiguration(((TRANSInputSplit)arg0).getConfDir()));
		} catch (WrongArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(-1);
		} catch (JDOMException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		TRANSInputSplit split = (TRANSInputSplit) arg0;
		split = (TRANSInputSplit) arg0;
		TransHostList list = split.getHosts();
		Vector<Host> h = list.getHosts();
		String hostname = InetAddress.getLocalHost().getHostName();
		Host use = null;
		for( int i = 0 ; i < h.size(); i++)
		{
			if( hostname.equals(h.get(i).getHost()))
			{
				use = h.get(i);
				break;
			}
		}
		if(use == null)
		{
			Random rand = new Random();
			use = h.get(rand.nextInt()%h.size());
		}
		dp = use.getDataProtocol();

		OptimusArray array = split.getArray();
		Partition p = new Partition(split.getZone().getId(),array.getId(),
				split.getPid(),new RID(0));

		System.out.println("Reading:"+p);
		StrideResultArrayWritable ret = dp.readStride(p, split.getPshape(), split.getStart(), 
				split.getOff(), split.getStride());
		result = (StrideResult[]) ret.getResult();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		if(this.cur >= this.result.length -1)
		{
			return false;
		}else{
			this.cur++;
			return true;
		}
	}

	@Override
	public IntWritable getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return new IntWritable(result[this.cur].getId());
	}

	@Override
	public StrideResult getCurrentValue() throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return result[this.cur];
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return (float) (this.cur/(result.length*1.0) + 1/3);
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

}
