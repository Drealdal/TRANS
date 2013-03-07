package TRANS.Client.creater;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import TRANS.util.ByteWriter;

public abstract class PartitionCreater {
	protected DataOutputStream cout = null;
	protected DataInputStream cin = null; 
	
	abstract public ByteWriter getWriter();
	abstract public boolean close();
}
