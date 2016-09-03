package org.kairosdb.congestion;

/**
 Created by bhawkins on 3/19/16.
 */
public class AdaptiveCongestionController implements CongestionController
{
	private int m_processCounter;
	private int m_maxProcesses;

	private Object m_processLock = new Object();
	private boolean m_shuttingDown = false;
	private boolean m_inSlowStart = true;

	private void processReturned()
	{
		synchronized (m_processLock)
		{
			m_processCounter --;
			m_processLock.notify();
		}
	}

	public AdaptiveCongestionController()
	{
	}

	public void shutdown()
	{
		m_shuttingDown = true;
		m_processLock.notifyAll();
	}

	@Override
	public ProcessTracker getProcessTracker()
	{
		synchronized (m_processLock)
		{
			while (m_processCounter >= m_maxProcesses)
			{
				try
				{
					m_processLock.wait();
				}
				catch (InterruptedException e)
				{
					e.printStackTrace();
				}

				if (m_shuttingDown)
					return null;
			}

			m_processCounter ++;
		}

		ProcessTimer pt = new ProcessTimer(this);
		pt.start();

		//Need an ability to mark certain timings with a version.
		//Do this durring slow start to track the new processes

		return pt;
	}

	public void finishedProcess(long microseconds)
	{
		processReturned();
		//Add microseconds to stats
	}

	public void failedProcess()
	{
		processReturned();
		//Initiate slow start
	}
}
