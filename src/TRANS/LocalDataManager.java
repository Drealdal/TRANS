package TRANS;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import TRANS.Array.Partition;
import TRANS.Exceptions.WrongArgumentException;


public class LocalDataManager {
	
	private String dataHome = null;
	void openPatition(Partition p) throws FileNotFoundException
	{
		p.setDataf(new RandomAccessFile(getPath( p), "r"));
	}
	void createPatition(Partition p) throws IOException
	{
		p.setDataf(new RandomAccessFile(getPath(p),"rw"));
	}
	String getPath(Partition p)
	{
		return new String(dataHome+"/"+p.getPartitionPath());
	}
	public LocalDataManager(String dataHome) throws WrongArgumentException
	{
		File dfile = new File(dataHome);
		if( dfile == null || !dfile.exists())
		{
			dfile.mkdir();
		}else if(dfile.exists()&&!dfile.isDirectory()){
			throw new WrongArgumentException("dataHome","Can not create Directory"+dataHome);
		}
		this.dataHome = dataHome;
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
