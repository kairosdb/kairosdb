//
//  LoggingUtils.java
//
// Copyright 2016, KairosDB Authors
//        
package org.kairosdb.util;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;

import static org.slf4j.LoggerFactory.getLogger;


public class LoggingUtils
{
	private LoggingUtils()
	{
	}

	/**
	 * Sets the logging level and returns the previous level.
	 * @param level level to change to
	 * @return previous level
	 */
	public static Level setLogLevel(Level level)
	{
		Level previous = level;
		org.slf4j.Logger logger = getLogger(Logger.ROOT_LOGGER_NAME);
		if (logger instanceof Logger)
		{
			Logger root = (Logger) logger;
			previous = root.getLevel();
			root.setLevel(level);
		}
		return previous;
	}

}