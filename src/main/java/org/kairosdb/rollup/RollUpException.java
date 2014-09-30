package org.kairosdb.rollup;

import org.kairosdb.core.exception.KairosDBException;

public class RollUpException extends KairosDBException
{
	public RollUpException(String message)
	{
		super(message);
	}

	public RollUpException(String message, Throwable cause)
	{
		super(message, cause);
	}

	public RollUpException(Throwable cause)
	{
		super(cause);
	}
}
