package TRANS.Data;

import TRANS.Array.OptimusShape;
import TRANS.Exceptions.WrongArgumentException;
import TRANS.util.OptimusData;

public class OptimusInterator {

	private OptimusData data = null;
	private OptimusShape rstart = null;
	private OptimusShape roff = null;
	private int[] jump = null;
	private int[] itr = null;
	private int pos = 0;
    
	public OptimusInterator(OptimusData data, OptimusShape start,
			OptimusShape off) {
		this.data = data;
		this.rstart = start;
		this.roff = off;

		jump = new int[this.rstart.getShape().length];
		int[] fsize = this.data.getOff().getShape();

		jump[0] = fsize[0];
		for (int i = 1; i < jump.length; i++) {
			jump[i] = fsize[i] * jump[i - 1];
		}

		itr = new int[jump.length];
	
		int [] fstart = data.getStart().getShape();
		int [] rdstart = rstart.getShape();
		for( int i =itr.length - 1 ;  i >= 0 ; --i )
		{
			pos = (pos == 0 ) ? rdstart[i] -fstart[i] : pos * fsize[i] + rdstart[i] - fstart[i];
		}
	
	}

	public boolean next() {
		int[] toff = this.roff.getShape();
		int l = itr.length - 1;
		if (itr[l] >= roff.getShape()[l]) {
			return false;
		}

		int j = 0;
		int len = toff.length - 1;
		while (j <= len) {
			itr[j]++;
			if (j != 0) {
				pos += jump[j - 1];
			} else {
				pos++;
			}
			if (itr[j] < toff[j]) {
				break;
			} else if (j == len) {
				break;
			} else {
				if (j == 0) {
					pos -= itr[j];
				} else {
					pos -= itr[j] * jump[j - 1];
				}
				itr[j] = 0;
			}
			j++;
		}
		if (itr[l] >= roff.getShape()[l]) {
			return false;
		}
		return true;
	}
	public double get() throws WrongArgumentException
	{
		return this.data.getDataByOff(pos);
	}

}
