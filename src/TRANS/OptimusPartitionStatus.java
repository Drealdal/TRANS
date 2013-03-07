package TRANS;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.io.Writable;

import TRANS.Array.Partition;


public class OptimusPartitionStatus implements Writable {
	int cur = 0;

	public Vector<Partition> getPartitions() {
		return partitions;
	}
	public void setPartitions(Vector<Partition> partitions) {
		this.partitions = partitions;
	}

	Vector<Partition> partitions = null;
	public OptimusPartitionStatus(){};
	public OptimusPartitionStatus(Vector<Partition> ps)
	{
		this.partitions = ps;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		if(this.partitions == null)
		{
			out.write(0);
			return;
		}
		int l = this.partitions.size();
		out.writeInt(l);
		for(int i = 0 ; i < l ; i++ )
		{
			this.partitions.get(i).write(out);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		int l  = in.readInt();
		this.partitions = new Vector<Partition>();
		for(int i = 0; i < l ; i++)
		{
			Partition p  = new Partition();
			p.readFields(in);
			this.partitions.add(p);
		}

	}

	public Partition getPartition(int i)
	{
		this.cur = i;
		return this.partitions.get(i);
	}
	public Partition nexPartition()
	{
		if(this.cur < this.partitions.size())
		{
			return this.partitions.get(this.cur++);
		}
		return null;
	}
	public boolean hasNext()
	{
		return (this.cur < this.partitions.size());
	}


}
