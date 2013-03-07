package TRANS.Calculator;

import org.apache.hadoop.io.Writable;

public abstract class OptimusCalculator implements Writable{

	abstract public double calcOne(double o1, double o2);
	abstract public double [] calcArray(double []a1, double []a2);
}
