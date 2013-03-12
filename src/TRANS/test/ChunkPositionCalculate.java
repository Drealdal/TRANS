package TRANS.test;

public class ChunkPositionCalculate {
	public static void main(String []args){
	int []vsize = {5,5,6};
	int []csize = {2,2,2};
	int []start = {1,4,5};
	
	long offset = getOffsetOfChunk(vsize,csize,start);
	long []s = getChunkByOffset(vsize,csize,offset);
	for(int i = 0; i < s.length; i++)
	{
		System.out.println("\t"+s[i]);
	}
	}
	static long getOffsetOfChunk(int []vsize, int[] csize, int []start)
	{
		int [] volume = new int [vsize.length];
		int []dsize = new int [vsize.length +1];
		dsize[vsize.length]=1;
		volume[0]=1;
		for(int i = 1; i < volume.length; i++)
		{
			volume[i] = volume[i-1]*vsize[i-1];
			
		}
		for(int i = vsize.length -1 ; i >= 0; i--)
		{
			dsize[i] = dsize[i+1] *( (vsize[i] - start[i]) > csize[i]? csize[i]:(vsize[i] - start[i]));
		}
		int offset = 0;
		for(int i = volume.length - 1; i >= 0 ; i--)
		{
			offset = offset + start[i] * volume[i]*dsize[i+1];
		}
		return offset;
	}
	
	static long [] getChunkByOffset(int []vsize, int []csize, long offset)
	{
		long []start = new long[vsize.length];
		
		int [] volume = new int [vsize.length];
		volume[0]=1;
		for(int i = 1; i < volume.length; i++)
		{
			volume[i] = volume[i-1]*vsize[i-1];
			
		}
		int dsize = 1;
		for(int i = volume.length - 1; i >= 0 ; i--)
		{
			start[i] = offset/(volume[i]*dsize);
			start[i] -= start[i]%csize[i];
			offset -=volume[i] * (start[i]*dsize);
			dsize *= (vsize[i] - start[i]) > csize[i] ? (vsize[i] - start[i]) : csize[i];
		}
		return start;
		
	}
}
