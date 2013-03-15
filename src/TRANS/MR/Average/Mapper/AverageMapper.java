package TRANS.MR.Average.Mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import TRANS.Data.Optimus1Ddata;
import TRANS.MR.io.AverageResult;
import TRANS.util.OptimusData;

/**
 * Mapper for the Average operator
 */
public class AverageMapper extends Mapper<Object, OptimusData, LongWritable, AverageResult> {
  public static enum InvalidCell { INVALID_CELL_COUNT } ;

  /**
   * Reduces values for a given key
   * @param key the Key for the given value being passed in
   * @param value an Array to process that corresponds to the given key 
   * @param context the Context object for the currently executing job
   */
  public void map(Object key, OptimusData value, Context context)
                  throws IOException, InterruptedException {
    AverageResult r = new AverageResult();
   	double []data = value.getData();
    
     for(int i = 0 ; i < data.length; i++)
     {
    	System.out.println(data[i]);
   	  	r.addValue(data[i]);
     }
     System.out.print(r);
     context.write(new LongWritable(1), r);
   }
      
}
