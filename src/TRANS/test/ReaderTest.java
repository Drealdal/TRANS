package TRANS.test;

import java.io.IOException;

import org.jdom2.JDOMException;

import TRANS.Array.OptimusZone;
import TRANS.Client.ZoneClient;
import TRANS.Client.Reader.PartitionReader;
import TRANS.Exceptions.WrongArgumentException;
import TRANS.util.OptimusConfiguration;

public class ReaderTest {

	/**
	 * @param args
	 * @throws JDOMException 
	 * @throws WrongArgumentException 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException, WrongArgumentException, JDOMException {
		// TODO Auto-generated method stub
		OptimusConfiguration conf = new OptimusConfiguration("./conf");
		PartitionReader reader = new PartitionReader(conf);
		
		int [] start = {0,5,0};
		int [] off = {900,111,11};
		ZoneClient zclient = new ZoneClient(conf);
		OptimusZone zone = zclient.openZone("test");
		if(zone == null)
		{
			System.out.print("UnCreated zone or unknown error happened");
		}
		double [] data = reader.readData(zone,"test3", start, off);
		for( int i = 0; i < data.length; i++)
		{
			System.out.print(data[i]+"\n");
		}
	}
	

}
