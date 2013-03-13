package TRANS.Array;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.io.Writable;


public class OptimusShapes implements Writable {

	@Override
	public String toString() {
		return "OptimusShapes [shapes=" + shapes + "]";
	}
	private Vector<int []> shapes = null;
	public OptimusShapes(){};
	public OptimusShapes(Vector<int []>shapes)
	{
		this.shapes = shapes;
		
	}
	@Override
	public void write(DataOutput out) throws IOException {
		
		if( this.shapes == null )
		{
			out.writeInt(0);
			return;
		}
		out.writeInt(this.shapes.size());
		for(int i = 0 ; i  < this.shapes.size() ; i++ )
		{
			new OptimusShape(this.shapes.get(i)).write(out);
		}

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int l = in.readInt();
		if(l == 0)
		{
			this.shapes = null;
			return;
		}
		this.shapes = new Vector<int []>();
		for(int i = 0 ; i < l ; i++)
		{
			OptimusShape shape = new OptimusShape();
			shape.readFields(in);
			shapes.add(shape.getShape());
			
		}
	}

	public Vector<int[]> getShapes() {
		return shapes;
	}
	public void setShapes(Vector<int[]> shapes) {
		this.shapes = shapes;
	}


}
