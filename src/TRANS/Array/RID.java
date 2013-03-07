package TRANS.Array;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class RID implements Writable {
	private int id = 0;
	
	public RID(){};
	public RID(int id)
	{
		this.id = id;
	}
	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.id);

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.id = in.readInt();
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
		RID other = (RID) obj;
		if (id != other.id)
			return false;
		return true;
	}
	@Override
	public String toString() {
		return "RID [id=" + id + "]";
	}

}
