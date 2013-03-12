package TRANS.MR;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.InputSplit;

import TRANS.Array.ArrayID;
import TRANS.Array.OptimusArray;
import TRANS.Array.OptimusShape;
import TRANS.Array.OptimusZone;
import TRANS.Array.PID;

public class TRANSInputSplit extends InputSplit implements Writable{

	public OptimusShape getStart() {
		return start;
	}
	public void setStart(OptimusShape start) {
		this.start = start;
	}
	public OptimusShape getOff() {
		return off;
	}
	public void setOff(OptimusShape off) {
		this.off = off;
	}
	public OptimusZone getZone() {
		return zone;
	}
	public void setZone(OptimusZone zone) {
		this.zone = zone;
	}
	public OptimusArray getArray() {
		return array;
	}
	public void setArray(OptimusArray array) {
		this.array = array;
	}
	public PID getPid() {
		return pid;
	}
	public void setPid(PID pid) {
		this.pid = pid;
	}

	private PID pid=null;
	private OptimusZone zone = null;
	private OptimusArray array = null;
	private OptimusShape start = null;
	private OptimusShape off = null;
	private String confDir = null;
	public TRANSInputSplit(){}
	public TRANSInputSplit(OptimusZone zone, OptimusArray array, PID pid,OptimusShape start, OptimusShape off,String confDir)
	{
		this.zone = zone;
		this.array = array;
		this.pid = pid;
		this.start = start;
		this.off = off;
		this.confDir = confDir;
	}
	public String getConfDir() {
		return confDir;
	}
	public void setConfDir(String confDir) {
		this.confDir = confDir;
	}
	@Override
	public long getLength() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		return 0;
	}

	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String []hosts = new String[1];
		hosts[0]="localhost";
		return hosts;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		pid = new PID();
		pid.readFields(arg0);
		zone = new OptimusZone();
		zone.readFields(arg0);
		array = new OptimusArray();
		array.readFields(arg0);
		this.confDir = WritableUtils.readString(arg0);
		this.start = new OptimusShape();
		this.start.readFields(arg0);
		
		this.off = new OptimusShape();
		this.off.readFields(arg0);
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		pid.write(arg0);
		zone.write(arg0);
		array.write(arg0);
		WritableUtils.writeString(arg0, this.confDir);
		this.start.write(arg0);
		this.off.write(arg0);
	}

}
