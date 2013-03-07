package TRANS.Array;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;


public class OptimusZone implements Writable {

	private String name = null;
	private ZoneID id = null;
	private OptimusShape size = null; // ����Ĵ�С,ÿ��zone�е����鶼��һ��Ĵ�С
	private OptimusShape pstep = null;// partition�Ĵ�С
	private OptimusShapes strategy = null; // �������
	
	
	public OptimusZone(){}
	public OptimusZone(String name,ZoneID id,OptimusShape size,OptimusShape step, OptimusShapes strategy)
	{
		this.name = name;
		this.id = id;
		this.size = size;
		this.strategy = strategy;
		this.pstep = step;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		
		WritableUtils.writeString(out, name);
		id.write(out);
		this.size.write(out);
		this.strategy.write(out);
		this.pstep.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		
		this.name = WritableUtils.readString(in);
		this.id = new ZoneID();
		this.id.readFields(in);
		this.size = new OptimusShape();
		this.size.readFields(in);
		this.strategy = new OptimusShapes();
		this.strategy.readFields(in);
		this.pstep = new OptimusShape();
		this.pstep.readFields(in);
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public ZoneID getId() {
		return id;
	}
	public void setId(ZoneID id) {
		this.id = id;
	}
	public OptimusShape getSize() {
		return size;
	}
	public void setSize(OptimusShape size) {
		this.size = size;
	}
	public OptimusShape getPstep() {
		return pstep;
	}
	public void setPstep(OptimusShape pstep) {
		this.pstep = pstep;
	}
	public OptimusShapes getStrategy() {
		return strategy;
	}
	public void setStrategy(OptimusShapes strategy) {
		this.strategy = strategy;
	}

}
