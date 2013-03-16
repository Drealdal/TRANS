package TRANS.test;

import java.util.Random;

public class TestConst {
	static int arrays = 0;
	static public String testZoneName="sci";
	static public String testArrayName="windspeed1";
	static public int [] srcStart={0,0,0,0};
	static public int [] vsize={8,8,8,8};
	static public int [] psize={4,4,4,4};
	static public int [] sshape={8,8,8,8};
	static public int [] dstShape1={4,1,1,1};
	static public int [] dstShape2={1,4,4,1};
	static public int [] overlap={2,2,2,2};
	
	static public int [] stride ={2,2,2,2};
	static public String nextArrayName()
	{
		Random r = new Random();
		return (testArrayName = testArrayName+(arrays++)+'_'+r.nextInt());
	}
	static public void setTestSuite(String name)
	{
		//TODO
	}
}
