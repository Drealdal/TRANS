package TRANS.util;

import java.io.IOException;

public interface ByteWriter {
	public void writeDouble(double d)throws IOException;
	public void writeDouble(double []d)throws IOException;
	public void close() throws IOException;
}
