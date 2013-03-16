package TRANS.MR.Median;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Writable;

import TRANS.Array.OptimusShape;

class OptimusResultKey implements Writable{
	@Override
	public String toString() {
		return "PresultKey [start=" + Arrays.toString(start) + ", shape="
				+ Arrays.toString(shape) + "]";
	}

	protected int[] start = null;
	protected int[] shape = null;
	public OptimusResultKey(){};
	public OptimusResultKey(int start[], int[] end) {
		this.start = start;
		this.shape = end;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		new OptimusShape(start).write(out);
		new OptimusShape(shape).write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		OptimusShape s = new OptimusShape();
		s.readFields(in);
		this.start = s.getShape();
		s.readFields(in);
		this.shape = s.getShape();
		
	}
}