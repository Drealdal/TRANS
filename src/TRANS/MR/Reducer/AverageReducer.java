package TRANS.MR.Reducer;
import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import TRANS.MR.io.AverageResult;

public class AverageReducer extends Reducer<LongWritable, AverageResult, IntWritable, DoubleWritable> {

public void reduce(LongWritable key, Iterable<AverageResult> values, 
             Context context)
             throws  InterruptedException, IOException {

AverageResult avgResult = new AverageResult();

//for (Result value : values) {
for (AverageResult value : values) {
	avgResult.addResult(value);
}

context.write(new IntWritable(1), new DoubleWritable(avgResult.getResult()) );

	
	}
}
