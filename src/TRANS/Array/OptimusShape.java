package TRANS.Array;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Writable;

public class OptimusShape implements Writable {

	@Override
	public String toString() {
		return "OptimusShape [shape=" + Arrays.toString(shape) + "]";
	}
	public int[] getShape() {
		return shape;
	}
	public void setShape(int[] shape) {
		this.shape = shape;
	}

	private int [] shape = null;
	public OptimusShape(){}
	public OptimusShape(int []shape)
	{
		this.shape = shape;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		
		if(this.shape == null)
		{
			out.writeInt(0);
			return;
		}
		out.writeInt(this.shape.length);
		for(int i = 0 ; i < this.shape.length; i++)
		{
			out.writeInt(this.shape[i]);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		
		int l = in.readInt();
		if(l == 0 )
		{
			this.shape = null;
			return;
		}
		this.shape = new int [l];
		for(int i = 0 ; i < l ; i ++)
		{
			this.shape[i] = in.readInt();
		}
		return;
	}

}
