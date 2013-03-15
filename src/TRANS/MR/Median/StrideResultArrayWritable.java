package TRANS.MR.Median;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class StrideResultArrayWritable implements Writable {

	public StrideResult[] getResult() {
		return result;
	}
	public void setResult(StrideResult[] result) {
		this.result = result;
	}
	StrideResult [] result = null;
	public StrideResultArrayWritable(){}
	public StrideResultArrayWritable(StrideResult []r){
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
		this.result = new StrideResult[len];
		for(int i = 0; i <len; i++)
		{
			result[i] = new StrideResult();
			result[i].readFields(in);
		}
	}

}
