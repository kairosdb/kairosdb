//
//  LoggingUtils.java
//
// Copyright 2013, Proofpoint Inc. All rights reserved.
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
		Logger root = (Logger) getLogger(Logger.ROOT_LOGGER_NAME);
		Level previous = root.getLevel();
		root.setLevel(level);
		return previous;
	}

}