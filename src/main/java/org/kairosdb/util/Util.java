/*
 * Copyright 2013 Proofpoint Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.kairosdb.util;


public class Util
{
	public static int compareLong(long l1, long l2)
	{
		long ret = l1 - l2;

		if (ret == 0L)
			return (0);
		else if (ret < 0L)
			return (-1);
		else
			return (1);
	}



	/**
	 Special thanks to Nadeau software consulting for publishing this code.
	 http://nadeausoftware.com/node/97
	 @param s string representation of number to parse
	 @return
	 */
	public static long parseLong( final CharSequence s )
	{
		if ( s == null )
			throw new NumberFormatException( "Null string" );

		// Check for a sign.
		long num  = 0;
		long sign = -1;
		final int len  = s.length( );
		final char ch  = s.charAt( 0 );
		if ( ch == '-' )
		{
			if ( len == 1 )
				throw new NumberFormatException( "Missing digits:  " + s );
			sign = 1;
		}
		else
		{
			final int d = ch - '0';
			if ( d < 0 || d > 9 )
				throw new NumberFormatException( "Malformed:  " + s );
			num = -d;
		}

		// Build the number.
		final long max = (sign == -1L) ?
				-Long.MAX_VALUE : Long.MIN_VALUE;
		final long multmax = max / 10;
		int i = 1;
		while ( i < len )
		{
			long d = s.charAt(i++) - '0';
			if ( d < 0L || d > 9L )
				throw new NumberFormatException( "Malformed:  " + s );
			if ( num < multmax )
				throw new NumberFormatException( "Over/underflow:  " + s );
			num *= 10;
			if ( num < (max+d) )
				throw new NumberFormatException( "Over/underflow:  " + s );
			num -= d;
		}

		return sign * num;
	}
}
