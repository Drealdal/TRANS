package TRANS.Client.creater;

import java.util.Random;

import TRANS.Array.DataChunk;


public class OptimusRandomScanner  implements OptimusScanner{

	public OptimusRandomScanner(){}
	@Override
	public double[] readChunkDouble(DataChunk chunk, String name) {
		double []data = new double [chunk.getSize()];
		Random r = new java.util.Random();
		for(int i =  0; i < data.length; i++)
		{
			data[i] = r.nextDouble();
		}
		return data;
	}

}
