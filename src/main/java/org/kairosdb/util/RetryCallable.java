package org.kairosdb.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

public abstract class RetryCallable implements Callable<Integer>
{
	public static final Logger logger = LoggerFactory.getLogger(RetryCallable.class);
	private int m_retries = -1;

	@Override
	public final Integer call() throws Exception
	{
		m_retries ++;

		if (m_retries > 0)
			logger.info("Retrying batch");

		retryCall();

		return m_retries;
	}

	public abstract void retryCall() throws Exception;
}
