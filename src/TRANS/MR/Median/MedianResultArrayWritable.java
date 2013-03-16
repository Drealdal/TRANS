package TRANS.MR.Median;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class MedianResultArrayWritable implements Writable {

	public StripeMedianResult[] getResult() {
		return result;
	}
	public void setResult(StripeMedianResult[] result) {
		this.result = result;
	}
	private StripeMedianResult [] result = null;
	public MedianResultArrayWritable(){}
	public MedianResultArrayWritable(StripeMedianResult []r){
		this.result = r;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		int len = 0;
		if(result == null)
		{
			WritableUtils.writeVInt(out, 0);
			return;
		}
		len =result.length;
		WritableUtils.writeVInt(out,len);
		for(int i =0; i < len; i++)
		{
			result[i].write(out);
		}
		
		
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		int len = WritableUtils.readVInt(in);
		if(len == 0)
		{
			return;
		}
		this.result = new StripeMedianResult[len];
		for(int i = 0; i <len; i++)
		{
			result[i] = new StripeMedianResult();
			result[i].readFields(in);
		}
	}

}
