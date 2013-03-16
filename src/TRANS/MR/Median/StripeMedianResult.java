package TRANS.MR.Median;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.Vector;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import TRANS.Array.OptimusShape;
import TRANS.util.Byte2DoubleReader;
import TRANS.util.OptimusDouble2ByteStreamWriter;

public class StripeMedianResult implements Writable {
	public int[] getStart() {
		return start;
	}

	public void setStart(int[] start) {
		this.start = start;
	}

	public int[] getStride() {
		return stride;
	}

	public void setStride(int[] stride) {
		this.stride = stride;
	}
	private int id=0;
	
	Vector<Double> data = new Vector<Double>();
	Set<OptimusResultKey> keys = new HashSet<OptimusResultKey>();
	private int volume = 1;
	private double result = 1;
	private boolean full = false;
	private int []start = null;
	private int []stride = null;
	public boolean isOverlaped( int []start, int []off)
	{
		boolean isIntersect = true;
		for (int i = 0; i < stride.length; i++) {
			if ((this.start[i] >= start[i] + off[i] || this.start[i]
					+ this.stride[i] <= start[i])) {
				isIntersect = false;
			}
		}
		return isIntersect;
	}
	public StripeMedianResult(){}
	public StripeMedianResult(int id,int []start, int []stride)
	{
		this.id = id;
		for(int i=0; i < stride.length; i++)
		{
			volume *= stride[i];
		}
		this.start = start;
		this.stride = stride;
	}
	public boolean isFull()
	{
		if(this.full) return true;
		return (this.data.size() >= this.volume);
	}
	public boolean contains(OptimusResultKey key)
	{
		return keys.contains(key);
	}
	public void add(double d)
	{
		data.add(d);
	}
	public void add(double []d)
	{
		for(int i=0;i<d.length;i++)
			data.add(d[i]);
	}
	public void add(StripeMedianResult r)
	{
		Vector<Double> data = r.getData();
		if(data != null)
			this.data.addAll(data);
	}
	public Vector<Double> getData() {
		return data;
	}
	public void setData(Vector<Double> data) {
		this.data = data;
	}
	public Set<OptimusResultKey> getKeys() {
		return keys;
	}
	public void setKeys(Set<OptimusResultKey> keys) {
		this.keys = keys;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		System.out.println(this);
		WritableUtils.writeVInt(out, this.id);
	
		new OptimusShape(this.start).write(out);
		this.full = this.isFull();
		new BooleanWritable(full).write(out);
		if(this.full)
		{
			
			new DoubleWritable(this.getResult()).write(out);
		}else{
			int len = this.data.size();
			
			WritableUtils.writeVInt(out, len);
			
			OptimusDouble2ByteStreamWriter writer = 
					new OptimusDouble2ByteStreamWriter(this.data.size() * 8,out);
			for(Double d:this.data)
				writer.writeDouble(d);
			writer.close();
		}
	}
	@Override
	public String toString() {
		return "StripeMedianResult [id=" + id + ", data=" + data + ", keys="
				+ keys + ", volume=" + volume + ", result=" + result
				+ ", full=" + full + ", start=" + Arrays.toString(start)
				+ ", stride=" + Arrays.toString(stride) + "]";
	}

	public double getResult()
	{
		if(this.isFull()&&this.data == null)
		{
			return this.result;
		}else{
			Collections.sort(this.data);
			this.result = this.data.get(this.data.size()/2);
			this.data = null;
		}
		return this.result;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.id = WritableUtils.readVInt(in);
		OptimusShape s = new OptimusShape();
		s.readFields(in);
		this.start = s.getShape();
		
		BooleanWritable b = new BooleanWritable();
		b.readFields(in);
		this.full =b.get();
		if(this.full)
		{
			DoubleWritable d = new DoubleWritable();
			d.readFields(in);
			this.result = d.get();
			this.data = null;
		}else{
			Byte2DoubleReader reader = new Byte2DoubleReader();
			int len = WritableUtils.readVInt(in);
			
			byte[] bdata = new byte[len*8];  
			in.readFully(bdata);
			reader.setData(bdata);
			this.data = new Vector<Double>();
			this.add(reader.readData());
		}
		
	}
	
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
}
