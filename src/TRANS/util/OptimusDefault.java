package TRANS.util;

public class OptimusDefault {

	
	public static String CATALOG_HOST = "localhost";
	public static int CATALOG_PORT = 2254;
	
	public static String DATA_HOST = "localhost";
	public static int DATA_PORT = 2255;
	
	public static String RELICATE_HOST = "localhost";
	public static int REPLICATE_PORT = 2256;
	
	public static String DATA_PATH = "./data";
	public static String META_PATH = "./meta";
	
	public static String META_FILENAME = "OptimusCatalog.data";
	
	public static String  OPTIMUSCONFIG = "./conf";
	public static int HEARTBEAT_TIME = 30; // minus
	public static String DATA_FILE_SUFIX = ".opt";
	
	public static int SOCKET_BUFFER_SIZE = 64*1024*1024; 
}
