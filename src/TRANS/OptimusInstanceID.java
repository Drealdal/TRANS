package TRANS;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
	
public class OptimusInstanceID implements Writable {

	private long id = 0;
	public OptimusInstanceID(){}
	public OptimusInstanceID(long id){
		this.id = id;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		WritableUtils.writeVLong(out, id);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub

		this.id = WritableUtils.readVLong(in);
	}
	public long getId() {
		return id;
	}
	public void setId(long id) {
		this.id = id;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (id ^ (id >>> 32));
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
		OptimusInstanceID other = (OptimusInstanceID) obj;
		if (id != other.id)
			return false;
		return true;
	}
	@Override
	public String toString() {
		return "OptimusInstanceID [id=" + id + "]";
	}
	

}
