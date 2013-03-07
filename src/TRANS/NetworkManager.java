package TRANS;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

public class NetworkManager {

	static public InetSocketAddress getLocalAddress(int port) throws UnknownHostException
	{
		return new InetSocketAddress(InetAddress.getLocalHost(),port);
	}
	static public InetSocketAddress getCatalogHost() throws UnknownHostException
	{
		return new InetSocketAddress(InetAddress.getByName("localhost"),2254);
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
