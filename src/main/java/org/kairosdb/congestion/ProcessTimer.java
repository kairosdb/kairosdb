package org.kairosdb.congestion;

import com.google.common.base.Stopwatch;

import java.util.concurrent.TimeUnit;

/**
 Created by bhawkins on 3/19/16.
 */
public class ProcessTimer implements ProcessTracker
{
	private Stopwatch m_timer;
	private AdaptiveCongestionController m_adaptiveCongestionController;

	public ProcessTimer(AdaptiveCongestionController congestionController)
	{
		m_timer = Stopwatch.createUnstarted();
		m_adaptiveCongestionController = congestionController;
	}

	public void start()
	{
		m_timer.reset();
		m_timer.start();
	}


	@Override
	public void finished()
	{
		m_timer.stop();
		long microseconds = m_timer.elapsed(TimeUnit.MICROSECONDS);
		m_adaptiveCongestionController.finishedProcess(microseconds);
	}


	@Override
	public void failed()
	{
		m_adaptiveCongestionController.failedProcess();
	}
}
