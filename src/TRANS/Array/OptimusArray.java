package TRANS.Array;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;


public class OptimusArray implements Writable{

	ZoneID zid = null;
	ArrayID id = null;
	String name = null;
	float devalue = 0;
	boolean deleted = false;

	public OptimusArray()	{}
	
	
	
	public OptimusArray(ZoneID zid,ArrayID id,String name,float devalue)
	{
		this.id = id;
		this.zid = zid;
		this.name = name;
		this.devalue = devalue;
		this.deleted = false;
	}
	@Override
	public void readFields(DataInput arg0) throws IOException {
		
		this.id = new ArrayID();
		id.readFields(arg0);
		this.zid = new ZoneID();
		this.zid.readFields(arg0);
		this.deleted = arg0.readBoolean();
		this.devalue = arg0.readFloat();
		this.name = WritableUtils.readString(arg0);
	}
	@Override
	public void write(DataOutput arg0) throws IOException {
		
		this.id.write(arg0);
		this.zid.write(arg0);
		arg0.writeBoolean(this.deleted);
		arg0.writeFloat(this.devalue);
		WritableUtils.writeString(arg0, this.name);
		
	}



	public ArrayID getId() {
		return id;
	}



	public void setId(ArrayID id) {
		this.id = id;
	}

	public boolean isDeleted() {
		return deleted;
	}



	public void setDeleted(boolean deleted) {
		this.deleted = deleted;
	}



	public ZoneID getZid() {
		return zid;
	}



	public void setZid(ZoneID zid) {
		this.zid = zid;
	}



	public String getName() {
		return name;
	}



	public void setName(String name) {
		this.name = name;
	}

	
}
