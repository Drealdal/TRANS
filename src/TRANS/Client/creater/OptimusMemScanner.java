package TRANS.Client.creater;

import TRANS.Array.DataChunk;

public class OptimusMemScanner implements OptimusScanner {

	double[] fdata = null;
	int[] fstart = null;
	int[] fsize = null;

	public OptimusMemScanner(double[] data, int[] fstart, int[] fsize) {
		this.fdata = data;
		this.fstart = fstart;
		this.fsize = fsize;
	}

	@Override
	public double[] readChunkDouble(DataChunk chunk, String name) {
		// TODO Auto-generated method stub
		double[] rdata = new double[chunk.getSize()];
		this.readFromMem(chunk.getStart(), chunk.getChunkSize(),
				chunk.getChunkSize(), rdata, chunk.getStart());
		return rdata;
	}

	public int readFromMem(int start[], int[] off, int[] tsize, double[] tdata,
			int[] tstart) {
		int size = 1;
		int fpos = 0;
		int tpos = 0;
		int[] fjump = new int[start.length];
		int[] djump = new int[start.length];
		for (int i = 0; i < start.length; i++) {
			size *= off[i];
			fpos = (fpos == 0) ? start[i] - fstart[i] : fpos * fsize[i]
					+ start[i] - fstart[i];
			tpos = (tpos == 0) ? start[i] - tstart[i] : tpos * tsize[i]
					+ start[i] - tstart[i];

		}
		fjump[start.length - 1] = fsize[start.length - 1];
		djump[start.length - 1] = tsize[start.length - 1];
		for (int i = start.length - 2; i >= 0; i--) {
			fjump[i] = fsize[i] * fjump[i + 1];
			djump[i] = tsize[i] * djump[i + 1];
		}
		if (size == 0) {
			return 0;
		}

		int len = start.length - 1;
		int[] iter = new int[len + 1];

		int j = 0;
		try {
			while (iter[0] < off[0]) {
				for (int i = 0; i < off[len]; i++) {
					tdata[tpos + i] = fdata[fpos + i];
				}
				j = len - 1;
				while (j >= 0) {
					iter[j]++;
					fpos += fjump[j + 1];
					tpos += djump[j + 1];
					if (iter[j] < off[j]) {
						break;
					} else if (j == 0) {
						break;
					} else {

						fpos -= iter[j] * fjump[j + 1];
						tpos -= iter[j] * djump[j + 1];

						iter[j] = 0;
					}
					j--;
				}
			}
		} catch (Exception e) {
			System.out.println("Shit!");
		}
		return size;
	}
}
