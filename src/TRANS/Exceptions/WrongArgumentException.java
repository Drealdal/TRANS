package TRANS.Exceptions;

public class WrongArgumentException extends Exception {

	private String MessageDes=null;
	private String ArgName = null;
	
	public WrongArgumentException(String argName, String Message)
	{
		this.MessageDes = Message;
		this.ArgName = argName;
	}
	
	@Override
	public void printStackTrace() {
		// TODO Auto-generated method stub
		super.printStackTrace();
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return new String(this.ArgName + " Get Wrong Value , the value get is " + this.MessageDes);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	

}
