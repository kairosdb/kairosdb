package org.kairosdb.util;

import java.util.concurrent.Callable;

public abstract class RetryCallable implements Callable<Integer>
{
	private int m_retries = -1;

	@Override
	public final Integer call() throws Exception
	{
		m_retries ++;

		if (m_retries > 0)
			System.out.println("Retrying batch");

		retryCall();

		return m_retries;
	}

	public abstract void retryCall() throws Exception;
}
