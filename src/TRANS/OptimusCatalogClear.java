package TRANS;

import java.io.IOException;

public class OptimusCatalogClear extends Thread {

	OptimusCatalog catalog = null;
	public OptimusCatalogClear(OptimusCatalog catalog)
	{
		this.catalog = catalog;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	@Override
	public void run() {
		try {
			this.catalog.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
