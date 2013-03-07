package TRANS.test.data;

import TRANS.Array.OptimusShape;
import TRANS.Data.*;
import TRANS.Exceptions.WrongArgumentException;
import TRANS.util.OptimusData;

public class InteratorTest {

	/**
	 * @param args
	 * @throws WrongArgumentException 
	 */
	public static void main(String[] args) throws WrongArgumentException {
		// TODO Auto-generated method stub
		
		double [] d = new double[64];
		for(int i = 0 ; i < d.length; i++ )
		{
			d[i] = i;
		}
		//public OptimusData(float []data,OptimusShape start, OptimusShape off,OptimusShape shape)
		int [] tstart = {0,0,0};
		int [] toff = {4,4,4};
		OptimusShape start = new OptimusShape(tstart);
		OptimusShape off = new OptimusShape(toff);
		OptimusData data = new OptimusData(d,start,off,null);
		int [] dstart = {3,3,0};
		int [] doff = {0,0,4};
		OptimusInterator itr = new OptimusInterator(data,new OptimusShape(dstart),new OptimusShape(doff)); 
		do
		{
			System.out.println(itr.get());
		}while(itr.next());
	}

}
