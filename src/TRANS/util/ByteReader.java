package TRANS.util;

import java.io.IOException;

public interface ByteReader {

	public  double[] readData();
	public void readFromin() throws IOException;
	public void readFromin(int len) throws IOException;
}
