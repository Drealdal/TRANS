package TRANS.Array;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class ZoneID implements Writable,Comparable<ZoneID> {


	int id;
	public ZoneID(){};
	public ZoneID(int id)
	{
		this.id = id;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		WritableUtils.writeVInt(out, this.id);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.id = WritableUtils.readVInt(in);
	}
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	

	@Override
	public int compareTo(ZoneID arg0) {
		// TODO Auto-generated method stub
		if(this.id == arg0.getId())
		{
			return 0;
		}
		return this.id > arg0.getId() ? -1 : 1;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + id;
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ZoneID other = (ZoneID) obj;
		if (id != other.id)
			return false;
		return true;
	}
	@Override
	public String toString() {
		return "ZoneID [id=" + id + "]";
	}

}
