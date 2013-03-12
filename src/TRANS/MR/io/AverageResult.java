package TRANS.MR.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * This class represents the results of an average operation.
 * It tracks the results of average for part of the data and allows
 * for accurrate aggregation of partial results into the final result.
 */
public class AverageResult implements Writable { 
  private double _currentValue;
  private int _valuesCombinedCount;

  private static final Log LOG = LogFactory.getLog(AverageResult.class);

  /**
   * Constructor
   */
  public AverageResult() {
	  this._currentValue = 0.0;
	  this._valuesCombinedCount = 0;
  }
  
  public void addValue(double d){
	  this._currentValue += d;
	  this._valuesCombinedCount++;
  }
  public void addResult(AverageResult r)
  {
	  this._currentValue += r.get_currentValue();
	  this._valuesCombinedCount += r.get_valuesCombinedCount();
  }
  public double getResult()
  {
	  return this._currentValue/this._valuesCombinedCount;
  }
  
public double get_currentValue() {
	return _currentValue;
}

public void set_currentValue(double _currentValue) {
	this._currentValue = _currentValue;
}

public int get_valuesCombinedCount() {
	return _valuesCombinedCount;
}

public void set_valuesCombinedCount(int _valuesCombinedCount) {
	this._valuesCombinedCount = _valuesCombinedCount;
}

@Override
public void write(DataOutput out) throws IOException {
	// TODO Auto-generated method stub
	WritableUtils.writeVInt(out, this._valuesCombinedCount);
	new DoubleWritable(this._currentValue).write(out);
}

@Override
public void readFields(DataInput in) throws IOException {
	// TODO Auto-generated method stub
	this._valuesCombinedCount = WritableUtils.readVInt(in);
	DoubleWritable d = new DoubleWritable();
	d.readFields(in);
	this._currentValue = d.get();
	
}


}
