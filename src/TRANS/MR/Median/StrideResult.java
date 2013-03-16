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
	
	Set<OptimusResultKey> contains = new HashSet<OptimusResultKey>();
	public boolean addResult(int []start,int[]off)
	{
		OptimusResultKey key = new OptimusResultKey(start,off);
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
			OptimusResultKey item = (OptimusResultKey)items.next();
			item.write(out);
		}
		super.write(out);
	}
	public void add(StrideResult r)
	{
		Set<OptimusResultKey> p = r.getContains();
		for(OptimusResultKey key: p)
		{
			if(this.contains.contains(key))
				continue;
			r.init(key.start, key.shape);

			if(!this.init(key.start, key.shape))
			{
				break;
			}
			this.contains.add(key);
			while(r.next()&&this.next())
			{
				this.set(r.get());
			}
		}
	}
	public Set<OptimusResultKey> getContains() {
		return contains;
	}
	public void setContains(Set<OptimusResultKey> contains) {
		this.contains = contains;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.contains = new HashSet<OptimusResultKey>();
		this.id = WritableUtils.readVInt(in);
		int len = WritableUtils.readVInt(in);
		
		for(int i = 0 ; i < len ; i ++)
		{
			OptimusResultKey key = new OptimusResultKey();
			key.readFields(in);
			contains.add(key);
		}
		super.readFields(in);
	}

}
