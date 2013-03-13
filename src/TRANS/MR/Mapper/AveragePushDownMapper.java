package TRANS.MR.Mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import TRANS.MR.io.AverageResult;
import TRANS.util.OptimusData;

public class AveragePushDownMapper extends Mapper<Object, AverageResult, LongWritable, AverageResult> {
		 

		  /**
		   * Reduces values for a given key
		   * @param key the Key for the given value being passed in
		   * @param value an Array to process that corresponds to the given key 
		   * @param context the Context object for the currently executing job
		   */
		  public void map(Object key, AverageResult value, Context context)
		                  throws IOException, InterruptedException {
		    
		     context.write(new LongWritable(1), value);
		   }
		     
}
