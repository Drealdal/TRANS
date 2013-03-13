package TRANS.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class TransHostList implements Writable{
	public Vector<Host> getHosts() {
		return hosts;
	}
	public void setHosts(Vector<Host> hosts) {
		this.hosts = hosts;
	}
	public Vector<Double> getCost() {
		return cost;
	}

	private Vector<Host> hosts = new Vector<Host>();
	private Vector<Double> cost = new Vector<Double>();
	public TransHostList(){}
	public void appendHost(Host h, double cost)
	{
		hosts.add(h);
		this.cost.add(cost);
	}
	public void setCost(Vector<Double> d)
	{
		this.cost = d;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		WritableUtils.writeVInt(out, hosts.size());
		for(int i = 0; i < hosts.size();i++)
		{
			hosts.get(i).write(out);
			new DoubleWritable(cost.get(i)).write(out);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		int len = WritableUtils.readVInt(in);
		DoubleWritable d = new DoubleWritable();
		for(int i = 0 ; i < len; i++)
		{
			Host h = new Host();
			h.readFields(in);
			hosts.add(h);
			d.readFields(in);
			cost.add(d.get());
			
		}
	}

}
