package TRANS.util;

import java.io.IOException;
import java.util.concurrent.Semaphore;

import TRANS.Array.ChunkTranslater;
import TRANS.Array.DataChunk;


public class OptimusTranslator extends Thread {

	private DataChunk src = null;
	private DataChunk dst = null;
	private double [] data = null;
	java.util.Queue<double []> datas= new java.util.ArrayDeque<double[]>();
	
	private Semaphore w = null;
	private int size = 0;
	ByteWriter writer = null;
	public OptimusTranslator(int size, DataChunk s, DataChunk dst, ByteWriter writer)
	{
		this.src = s;
		this.dst = dst;
		this.data = new double [size];
		this.size = size;
		this.writer = writer;
		w = new Semaphore(0);
	}
	public double [] getData()
	{
		return this.data;
	}
	public void write(double [] d)
	{
		synchronized(this.datas){
			
			this.datas.add(d);
		}
			w.release();
	}
	@Override
	public void run() {
		int translated = 0;
		double [] tdouble = null;
		while( translated < this.size )
		{
			try {
				w.acquire();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				continue;
			}
			synchronized(this.datas)
			{
				tdouble = this.datas.remove();
			}
			
			for(int i = 0; i < tdouble.length; i++)
			{
				src.getChunkByOff(translated++);
				dst.getChunkByOff(ChunkTranslater.offTranslate(src));
				data[dst.getOffset()] = tdouble[i];
			}
		}
		try {
			this.writer.writeDouble(this.data);
			this.writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
}
