package TRANS.util;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.ipc.RPC;

import TRANS.OptimusInstanceID;
import TRANS.Protocol.OptimusCalculatorProtocol;
import TRANS.Protocol.OptimusDataProtocol;

public class Host implements Writable {

	@Override
	public String toString() {
		return "Host [host=" + host + ", RplicatePort=" + rplicatePort
				+ ", instanceId=" + instanceId.toString() + "]";
	}
	
	private String host = null;
	private boolean isDead = false;
	private long liveTime = 0;
	private int rplicatePort = 0;
	private OptimusInstanceID instanceId = null;
	private int dataPort = 0;
	
	Socket socket = null;
	OptimusConfiguration conf = null;

	public Host() {}

	public Host( String host, int replicatePort, int dataPort, OptimusInstanceID instanceId) {
		this.host = host;
		this.rplicatePort = replicatePort;
		this.dataPort = dataPort;
		this.instanceId = instanceId;
		this.liveTime = System.currentTimeMillis();
	}

	public void ConnectReplicate() throws UnknownHostException, IOException {
		socket = new Socket(this.host, this.rplicatePort);
		socket.setSendBufferSize(OptimusDefault.SOCKET_BUFFER_SIZE);
		
	}

	public void closeReplicate() throws IOException {
		socket.close();
	}

	public DataOutputStream  getReplicateWriter() throws IOException {
		return new DataOutputStream(socket.getOutputStream());
	}

	public DataInputStream getReplicateReply() throws IOException {
		return new DataInputStream(socket.getInputStream());
	}
	
	public OptimusDataProtocol getDataProtocol() throws IOException
	{
		//TODO
		
		return (OptimusDataProtocol) RPC.waitForProxy(OptimusDataProtocol.class,
				OptimusDataProtocol.versionID,
				new InetSocketAddress(this.host,this.dataPort), new Configuration());

	}
	public OptimusCalculatorProtocol getCalculateProtocol() throws IOException
	{
		//TODO
		
		return (OptimusCalculatorProtocol) RPC.waitForProxy(OptimusCalculatorProtocol.class,
				OptimusCalculatorProtocol.versionID,
				new InetSocketAddress(this.host,this.dataPort), new Configuration());

	}


	@Override
	public void readFields(DataInput arg0) throws IOException {
	

		this.instanceId =new OptimusInstanceID();
		this.instanceId.readFields(arg0);
		
		rplicatePort = WritableUtils.readVInt(arg0);
		host = WritableUtils.readString(arg0);
		this.dataPort = WritableUtils.readVInt(arg0);
		
		this.liveTime = WritableUtils.readVLong(arg0);
		this.isDead = WritableUtils.readVInt(arg0) == 1 ? true : false; 
		
		
	}
	public void update()
	{
		this.isDead = false;
		this.liveTime = System.currentTimeMillis();
	}
	@Override
	public void write(DataOutput arg0) throws IOException {
	

		this.instanceId.write(arg0);
		WritableUtils.writeVInt(arg0, rplicatePort);
		
		WritableUtils.writeString(arg0, host);
		WritableUtils.writeVInt(arg0,this.dataPort);
		this.liveTime = System.currentTimeMillis(); // update the live time
		WritableUtils.writeVLong(arg0, this.liveTime);
		WritableUtils.writeVInt(arg0, this.isDead ? 1 : 0);
	}

	public int compareTo(Object arg0) {

		Host ob = (Host) arg0;
		if (this.host == ob.host && this.rplicatePort == ob.rplicatePort)
			return 0;
		return -1;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return rplicatePort;
	}

	public void setPort(int port) {
		this.rplicatePort = port;
	}

	public OptimusInstanceID getInstanceId() {
		return instanceId;
	}

	public void setInstanceId(OptimusInstanceID instanceId) {
		this.instanceId = instanceId;
	}

	public long getLiveTime() {
		return liveTime;
	}

	public void setLiveTime(long liveTime) {
		this.liveTime = liveTime;
	}

	public boolean isDead() {
		return isDead;
	}

	public void setDead(boolean isDead) {
		this.isDead = isDead;
	}
}
