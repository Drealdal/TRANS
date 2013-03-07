package TRANS.Array;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class PID implements Writable {

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
		PID other = (PID) obj;
		if (id != other.id)
			return false;
		return true;
	}
	private int id = 0;
	public PID(){};
	public PID(int id)
	{
		this.id = id;
	}
	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		id = WritableUtils.readVInt(arg0);
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		WritableUtils.writeVInt(arg0, id);
	}

	public int getId()
	{
		return id;
	}
	public void setId(int id)
	{
		this.id = id;
	}
	public void increament()
	{
		this.id++;
	}
	@Override
	public String toString() {
		return "PID [id=" + id + "]";
	}
}
