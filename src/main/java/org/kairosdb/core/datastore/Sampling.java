//
// Sampling.java
//
// Copyright 2013, NextPage Inc. All rights reserved.
//

package org.kairosdb.core.datastore;

public class Sampling extends Duration
{
	public Sampling()
	{
		super();
	}

	public long getSampling()
	{
		long val = value;
		switch (unit)
		{
			case MILLISECONDS:
				break;
			case SECONDS: val *= 1000;
				break;
			case MINUTES: val *= (60 * 1000);
				break;
			case HOURS: val *= (60 * 60 * 1000);
				break;
			case DAYS: val *= (24 * 60 * 60 * 1000);
				break;
			case WEEKS: val *= (7 * 24 * 60 * 60 * 1000);
				break;
			case YEARS: val *= (365 * 7 * 24 * 60 * 60 * 1000);
				break;
		}

		return (val);
	}
}
