package TRANS.test;

import java.io.IOException;
import java.util.Vector;
import java.util.concurrent.TimeUnit;

import org.jdom2.JDOMException;

import TRANS.Array.DataChunk;
import TRANS.Array.OptimusZone;
import TRANS.Client.ArrayCreater;
import TRANS.Client.ZoneClient;
import TRANS.Client.creater.OptimusMemScanner;
import TRANS.Client.creater.OptimusScanner;
import TRANS.Exceptions.WrongArgumentException;
import TRANS.util.OptimusConfiguration;

public class ClientTest {
/*
	static public int readFromMem(int start [] , int [] off,int []fsize,int []tsize, 
			float [] fdata,int [] fstart, float[] tdata, int []tstart)
	{
		int size = 1;
		int fpos = 0;
		int tpos = 0 ;
		int [] fjump = new int [start.length];
		int [] djump = new int [start.length];
		for( int i =start.length - 1 ;  i >= 0 ; --i )
		{
			size *= off[i];
			fpos = (fpos == 0 ) ? start[i] -fstart[i] : fpos * fsize[i+1] + start[i] - fstart[i];
			tpos = (tpos == 0 ) ? start[i] - tstart[i] : tpos * tsize[i] + start[i] - tstart[i];
			
		}
		fjump[0] = fsize[0];
		djump[0] = tsize[0];
		for(int i = 1; i < start.length; i++)
		{
			fjump[i] = fsize[i] * fjump[i - 1];
			djump[i] = tsize[i] * djump[i - 1];
		}
		if(size == 0)
		{
			return 0;
		}
		
		int len = start.length - 1;
		int [] iter = new int [len + 1];
		
		int j =  0;
		while(iter[len] < off[len])
		{
			for(int i = 0 ; i < off[0]; i++ )
			{
				tdata[tpos+i] = fdata[fpos+i];
			}
			j = 1;
			while( j <= len )
			{
				iter[j]++;
				fpos += fjump[j - 1];
				tpos += djump[j - 1];
				if(iter[j] < off[j])
				{
					break;
				}else if(j == len){
					break;
				}else{
					
					fpos -= iter[j] * fjump[j - 1];
					tpos -= iter[j] * djump[j - 1];
					
					iter[j] = 0 ;
				}
				j++;
			}
		}
		return size;
	}
	*/
	/**
	 * @param args
	 * @throws JDOMException 
	 * @throws WrongArgumentException 
	 * @throws IOException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, WrongArgumentException, JDOMException, InterruptedException {
		// TODO Auto-generated method stub
		int [] srcStart = {0,0};
		int [] vsize = TestConst.srcStart;
		int [] shape = TestConst.psize;
		int [] srcShape = TestConst.sshape;
		int [] dstShape = TestConst.dstShape1;
		int [] dstShape2 = TestConst.dstShape2;
		int [] overlap = TestConst.overlap;
		Vector<int []>strategy = new Vector<int []>();
		strategy.add(dstShape);
		strategy.add(dstShape2);
		strategy.add(srcShape);

		String zoneName = TestConst.testZoneName;
		String arrayName = TestConst.testArrayName;
		
		OptimusConfiguration conf = new OptimusConfiguration("./conf");
		ZoneClient zcreater = new ZoneClient(conf);
		OptimusZone zone = zcreater.openZone(zoneName);
		if(zone == null)
		{
			zone = zcreater.createZone(zoneName, vsize, shape, strategy);
		}
		if(zone == null)
		{
			System.out.println("Unknown error");
			return;
		}
		long btime = System.currentTimeMillis();
		ArrayCreater creater = new ArrayCreater(conf,zone,srcShape,arrayName,1,0);
		creater.create();
		
		DataChunk chunk = new DataChunk(vsize,shape);
		int len = 1;
		for(int i = 0; i < vsize.length; i++)
		{
			len *= vsize[i];
		}
		double [] srcData = new double[len];
		for(int i = 0  ; i < srcData.length; i++)
		{
			srcData[i] = i;
		}
		
		OptimusScanner scanner = new OptimusMemScanner(srcData,srcStart,vsize);
		do{
		
			System.out.println(chunk);
			chunk.setOverlap(overlap);
			creater.createPartition(scanner,chunk,"testArray");
		}while(chunk.nextChunk());
		creater.close(1000, TimeUnit.SECONDS);
		long etime = System.currentTimeMillis();
		System.out.println("Total time used:"+(etime-btime));
	}
	

}
