package TRANS.Array;
public class ChunkTranslater {

	double [] translate(int []size, int []to,double [] data)
	{
		double [] ret = new double[data.length];
		return ret;
	}
	// ����һ��chunk�ڰ������ȵ�����е�offset
	static public int offTranslate(DataChunk chunk)
	{
		
		int l = chunk.getVsize().length;
		int [] chunkpos = new int [l];
		int inChunk = chunk.getInChunk();
		int []csize = chunk.getChunkSize();
		for(int i = l - 1 ; i >= 0 ; i--)
		{
			chunkpos[i] = chunk.getStart()[i] + inChunk%csize[i]; 
			inChunk/=csize[i];
		}
		int start = 0;
		for( int i = 0 ; i < l  ; i++)
		{
			start = chunkpos[i]  + start * chunk.getVsize()[i];
		}
		return start;
	}
	// ����chunk�洢�е�λ��
	static int getOffInChunk(DataChunk chunk)
	{
		return chunk.getOffset();
	}
	
	
	double readDouble(double []data, DataChunk chunk)
	{
		
		return data[offTranslate(chunk)];
	}
	
	static public void main(String []args)
	{
		double [] data = new double[64]; // 64 MB
		for(int i = 0 ; i < 64 ; i++ )
			data[i] = i;
		double [] cdata = new double[64];
		
		int []vsize = {4,4,4};
		int []chunkSteps = {2,2,2};
		ChunkTranslater t = new ChunkTranslater();
		DataChunk chunk = new DataChunk(vsize,chunkSteps);
		
		boolean nex = true;
		int i = 0;
		while(nex)
		{
			cdata[i++] = t.readDouble(data, chunk);
			System.out.print(cdata[i-1]+" ");
			nex = chunk.nextPos();
			if(chunk.inChunk == 0 )
			{
				System.out.print("\n");
			}
		}
	
		for( i = 0; i < 64 ; i++)
		{
			chunk.getChunkByOff(i);
			
			System.out.print(" "+cdata[getOffInChunk(chunk)]);
		}
	}
}
