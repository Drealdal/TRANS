package TRANS.util;

import java.io.IOException;
import java.util.concurrent.Semaphore;

public class OptimusWriter extends Thread{

	private ByteWriter writer = null;
	private int ByteToWrite = 0;
	private double []data = null;
	private Semaphore w = null;
	private Semaphore r = null;
	public OptimusWriter(ByteWriter writer, int size)
	{
		this.writer = writer;
		this.ByteToWrite = size;
		r = new Semaphore(0);
		w = new Semaphore(1);
	}
	public void write(double []d) throws InterruptedException
	{
			w.acquire();
			this.data = d;
			r.release();
	}
	public void write(OptimusTranslator trans) throws InterruptedException
	{
		long b = System.currentTimeMillis();
		trans.join();
		System.out.println("Waiting trans"+(System.currentTimeMillis() - b));
		this.write(trans.getData());
	}
	@Override
	public void run() {
		int writen = 0;
		while(writen < this.ByteToWrite )
		{
			try{
				r.acquire();
				synchronized(this){
				writer.writeDouble(data);
				writen += data.length * 8;
				data = null;
				w.release();
				}
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		try {
			this.writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
}
