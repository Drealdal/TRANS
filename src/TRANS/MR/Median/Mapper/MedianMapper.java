package TRANS.MR.Median.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Vector;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;
import org.jdom2.JDOMException;

import TRANS.Array.DataChunk;
import TRANS.Array.OptimusArray;
import TRANS.Array.OptimusZone;
import TRANS.Client.ZoneClient;
import TRANS.Exceptions.WrongArgumentException;
import TRANS.MR.Median.StrideResult;
import TRANS.MR.Median.StripeMedianResult;
import TRANS.Protocol.OptimusCatalogProtocol;
import TRANS.util.OptimusConfiguration;

/**
 * Mapper for the Average operator
 */
public class MedianMapper extends Mapper<IntWritable, StripeMedianResult, IntWritable, StripeMedianResult> {
  public static enum InvalidCell { INVALID_CELL_COUNT } ;

  Vector<Integer> fullIds = new Vector<Integer>();
  DataChunk chunk = null;
  @Override
protected void setup(Context context) throws IOException, InterruptedException {
	// TODO Auto-generated method stub
	/*	JobConf conf = (JobConf)context.getConfiguration();
		String ozname = conf.get("TRANS.output.zone.name");
		String oaname = conf.get("TRANS.output.zone.name");
		String confDir= conf.get("TRANS.conf.dir");
		
		ZoneClient zclient = null;
		try {
			zclient = new ZoneClient(new OptimusConfiguration(confDir));
		} catch (WrongArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(-2);
		} catch (JDOMException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		OptimusZone zone = zclient.openZone(ozname);
		if(zone == null)
		{
			System.out.print("UnCreated zone or unknown error happened");
			System.exit(-1);
		}
		OptimusCatalogProtocol ci = zclient.getCi();
		OptimusArray array = null;
		try {
			array = ci.openArray(zone.getId(),new Text(oaname));
		} catch (WrongArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(-1);
		}
	  chunk = new DataChunk(zone.getSize().getShape(),zone.getPstep().getShape());*/
	  super.setup(context);
}


@Override
protected void cleanup(Context context) throws IOException,
		InterruptedException {

	
	
	
	super.cleanup(context);
}


/**
   * Reduces values for a given key
   * @param key the Key for the given value being passed in
   * @param value an Array to process that corresponds to the given key 
   * @param context the Context object for the currently executing job
   */
  public void map(IntWritable key, StripeMedianResult value, Context context)
                  throws IOException, InterruptedException {
	
  //  if(value.isFull())
 //   {
   // 	double []data = value.getData();
  //  	Arrays.sort(data);
  //  }else{
	//  if(value.isFull())
	//  {
		  
//	  }else{
		  context.write(key, value);
	//  }
  //  }
   }
      
}
