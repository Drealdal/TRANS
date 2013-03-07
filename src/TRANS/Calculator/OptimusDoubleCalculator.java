package TRANS.Calculator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OptimusDoubleCalculator extends OptimusCalculator {

	private int op;

	public OptimusDoubleCalculator(){};
	public OptimusDoubleCalculator(char op){
		this.op = op;
	};
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(op);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.op = in.readInt();
	}

	@Override
	public double calcOne(double o1, double o2) {
		// TODO Auto-generated method stub
		return o1+o2;
	}

	@Override
	public double []calcArray(double a1[], double []a2)
	{
		switch(op)
		{
		case '+':
			return calcArrayAdd(a1,a2);
		case '-':
			return calcArraySub(a1,a2);
		case '*':
			return calcArrayMul(a1,a2);
		default:
				break;
		}
		return null;
	}
	
	private double[] calcArrayAdd(double[] a1, double[] a2) {
		// TODO Auto-generated method stub
		double [] a3 = new double [a1.length];
		for(int i = 0; i < a1.length; i++)
		{
			a3[i] = a1[i]+a2[i];
		}
		return a3;
	}
	private double[] calcArraySub(double[] a1, double[] a2) {
		// TODO Auto-generated method stub
		double [] a3 = new double [a1.length];
		for(int i = 0; i < a1.length; i++)
		{
			a3[i] = a1[i]-a2[i];
		}
		return a3;
	}
	private double[] calcArrayMul(double[] a1, double[] a2) {
		// TODO Auto-generated method stub
		double [] a3 = new double [a1.length];
		for(int i = 0; i < a1.length; i++)
		{
			a3[i] = a1[i]*a2[i];
		}
		return a3;
	}
	
}
