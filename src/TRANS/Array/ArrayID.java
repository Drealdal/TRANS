package TRANS.Array;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class ArrayID implements Writable {
	int arrayId = 0;

	public ArrayID(int arrayId)
	{
		this.arrayId = arrayId;
	}
	
	public ArrayID(){
		
	};
	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		this.arrayId = arg0.readInt();
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		arg0.writeInt(this.arrayId);
	}

	public int getArrayId() {
		return arrayId;
	}

	public void setArrayId(int arrayId) {
		this.arrayId = arrayId;
	}


	@Override
	public String toString() {
		return "ArrayID [arrayId=" + arrayId + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + arrayId;
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
		ArrayID other = (ArrayID) obj;
		if (arrayId != other.arrayId)
			return false;
		return true;
	}
	
	
}
