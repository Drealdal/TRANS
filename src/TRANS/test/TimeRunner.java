package TRANS.test;

import java.util.Timer;
import java.util.TimerTask;

public class TimeRunner extends TimerTask{

	int who;
	public TimeRunner(int id)
	{
		this.who = id;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		 Timer timer = new Timer();
		 timer.schedule( new TimeRunner(1), 1,10 );
         timer.schedule(new TimeRunner(2), 1,10 );
       
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		System.out.println(this.who + " say shit!");
	}

}
