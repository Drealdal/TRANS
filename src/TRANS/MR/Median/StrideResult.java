package TRANS.MR.Median;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import TRANS.Array.OptimusShape;
import TRANS.util.TRANSDataIterator;

public class StrideResult extends TRANSDataIterator {
	
	@Override
	public String toString() {
		return "StrideResult [id=" + id + ", result=" + result + ", contains="
				+ contains + super.toString() + "]";
	}
	private int id = 0;
	private double result = -1;
	public double getResult() {
		return result;
	}
	public void setResult(double result) {
		this.result = result;
	}
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public StrideResult(){};
	public StrideResult(double []data, int []start, int []shape)
	{
		super(data,start,shape);
	}
	class PresultKey implements Writable{
		protected int[] start = null;
		protected int[] shape = null;
		public PresultKey(){};
		public PresultKey(int start[], int[] end) {
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
	Set<PresultKey> contains = new HashSet<PresultKey>();
	public boolean addResult(int []start,int[]off)
	{
		PresultKey key = new PresultKey(start,off);
		if(this.contains.contains(key))
		{
			return false;
		}
		this.contains.add(key);
		return true;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		WritableUtils.writeVInt(out, this.id);
	
		WritableUtils.writeVInt(out, contains.size());
		for(Iterator items = contains.iterator(); items.hasNext();){
			PresultKey item = (PresultKey)items.next();
			item.write(out);
		}
		super.write(out);
	}
	public void add(StrideResult r)
	{
		Set<PresultKey> p = r.getContains();
		for(PresultKey key: p)
		{
			r.init(key.start, key.shape);
			System.out.println(super.toString());
			if(!super.init(key.start, key.shape))
			{
				break;
			}
			while(r.next()&&this.next())
			{
				this.set(r.get());
			}
		}
	}
	public Set<PresultKey> getContains() {
		return contains;
	}
	public void setContains(Set<PresultKey> contains) {
		this.contains = contains;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.id = WritableUtils.readVInt(in);
		int len = WritableUtils.readVInt(in);
		
		for(int i = 0 ; i < len ; i ++)
		{
			PresultKey key = new PresultKey();
			key.readFields(in);
			contains.add(key);
		}
		super.readFields(in);
	}

}
