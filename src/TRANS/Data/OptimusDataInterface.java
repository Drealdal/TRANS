package TRANS.Data;

import java.io.DataInput;
import java.io.DataOutput;

public interface OptimusDataInterface {
	public OptimusType getType();
	public Object getData();
	public void write(DataOutput out);
	public void readField(DataInput in);
	
	public Object readElement(DataInput in);
	public void  writeElemtnt(DataOutput out);

}
