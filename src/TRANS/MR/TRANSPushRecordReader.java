package TRANS.MR;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Random;
import java.util.Vector;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.jdom2.JDOMException;

import TRANS.Array.OptimusArray;
import TRANS.Array.PID;
import TRANS.Client.Reader.PartitionReader;
import TRANS.Exceptions.WrongArgumentException;
import TRANS.MR.io.AverageResult;
import TRANS.Protocol.OptimusDataProtocol;
import TRANS.util.Host;
import TRANS.util.OptimusConfiguration;
import TRANS.util.TransHostList;

public class TRANSPushRecordReader extends RecordReader<PID, AverageResult> {
	private boolean readed = false;
	OptimusDataProtocol dp  = null;
	PartitionReader reader = null;
	TRANSInputSplit split = null;
	@Override
	public void initialize(InputSplit arg0, TaskAttemptContext context)
		throws IOException, InterruptedException {
			try {
				reader = new PartitionReader(new OptimusConfiguration(((TRANSInputSplit)arg0).getConfDir()));
			} catch (WrongArgumentException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.exit(-1);
			} catch (JDOMException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			split = (TRANSInputSplit) arg0;
			TransHostList list = split.getHosts();
			Vector<Host> h = list.getHosts();
			String hostname = InetAddress.getLocalHost().getHostName();
			Host use = null;
			for( int i = 0 ; i < h.size(); i++)
			{
				if( hostname.equals(h.get(i).getHost()))
				{
					use = h.get(i);
					break;
				}
			}
			if(use == null)
			{
				Random rand = new Random();
				use = h.get(rand.nextInt()%h.size());
			}
			dp = use.getDataProtocol();
		}
		

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		if(this.readed)
			return false;
			else{
				this.readed = true;
				return true;
			}
			}

	@Override
	public PID getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public AverageResult getCurrentValue() throws IOException,
			InterruptedException {
		OptimusArray array = split.getArray();
		return  dp.readAverage(array.getId(), split.getPid(), split.getPshape(), split.getStart(), split.getOff());
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

}
