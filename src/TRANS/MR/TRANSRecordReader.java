package TRANS.MR;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.jdom2.JDOMException;

import TRANS.Array.OptimusArray;
import TRANS.Array.OptimusZone;
import TRANS.Array.PID;
import TRANS.Client.Reader.PartitionReader;
import TRANS.Data.Optimus1Ddata;
import TRANS.Exceptions.WrongArgumentException;
import TRANS.util.OptimusConfiguration;

public class TRANSRecordReader extends RecordReader<PID, Optimus1Ddata> {
	
	PartitionReader reader = null;
	TRANSInputSplit split = null;
	boolean readed = false;
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public PID getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return split.getPid();
	}

	@Override
	public Optimus1Ddata getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		OptimusZone zone = split.getZone();
		OptimusArray array = split.getArray();
		try {
			return new Optimus1Ddata(reader.readData(zone,array.getName(), split.getStart().getShape(), split.getOff().getShape()));
		} catch (WrongArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void initialize(InputSplit arg0, TaskAttemptContext arg1)
			throws IOException, InterruptedException {
		try {
			reader = new PartitionReader(new OptimusConfiguration(null));
		} catch (WrongArgumentException | JDOMException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(-1);
		}
		split = (TRANSInputSplit) arg0;
		
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if(this.readed)
		return false;
		else{
			this.readed = true;
			return true;
		}
	}

}
