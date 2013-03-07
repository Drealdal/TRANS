package TRANS.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import TRANS.Array.OptimusShape;
import TRANS.Exceptions.WrongArgumentException;


/**
 * @author foryee
 *
 */
public class OptimusData implements Writable {
	private OptimusShape start;
	private OptimusShape off;
	private OptimusShape shape;
	double [] data = null;
	
	
	public OptimusData(){}
	public OptimusData(double []data,OptimusShape start, OptimusShape off,OptimusShape shape)
	{
		this.data = data;
		this.start = start;
		this.off = off;
		this.shape = shape;
	}
	public OptimusShape getShape() {
		return shape;
	}
	public void setShape(OptimusShape shape) {
		this.shape = shape;
	}
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
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		if(this.data == null)
		{
			out.writeInt(0);
			return;
		}
		out.writeInt(this.data.length);
		for(int i = 0 ; i < this.data.length; i++)
		{
			//out.writeFloat(this.data[i]);
			out.writeDouble(this.data[i]);
		}
			
	}
	public double[] getData() {
		return data;
	}
	public void setData(double[] data) {
		this.data = data;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		int l = in.readInt();
		this.data = new double [l];
		for(int i = 0; i < l; i++)
		{
			this.data[i] = in.readDouble();
		}
	}
	
	/**
	 * @param off
	 * @param inPartiton: off ��partition�ڲ���offset ����ȫ�ֵ�offset
	 * @return
	 * @throws WrongArgumentException 
	 */
	public double getData(OptimusShape off,boolean inPartition) throws WrongArgumentException
	{
		int []roff = new int [this.off.getShape().length];
		int [] toff = off.getShape();
		int [] start = off.getShape();
		int [] size = this.shape.getShape();
		if( !inPartition )
		{
			for( int i = 0 ; i < roff.length; i++ )
			{
				roff[i] = toff[i] - start[i];
				if( roff[i] < 0 )
				{
					throw new WrongArgumentException("off","rang range to read");
				}
			}
		}else{
			for( int i = 0 ; i < roff.length; i++ )
			{
				roff[i] = toff[i];
			}
		}
		int offset = roff[roff.length - 1];
		for( int  i = roff.length - 2; i>=0; i--)
		{
			offset = offset * size[ i +1 ] + roff[i];
		}
		if(offset >= this.data.length)
		{
			throw new WrongArgumentException("off","rang range to read");
		}
		return this.data[offset];
	}

	public double getDataByOff(int off) throws WrongArgumentException
	{
		if(off >= this.data.length)
		{
			throw new WrongArgumentException("off","rang range to read");
		}
		return this.data[off];
	}

}
