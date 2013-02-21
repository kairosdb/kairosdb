// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.util;


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
	 * Parses an integer value as a long from the given character sequence.
	 * <p>
	 * This is equivalent to {@link Long#parseLong(String)} except it's up to
	 * 100% faster on {@link String} and always works in O(1) space even with
	 * {@link StringBuilder} buffers (where it's 2x to 5x faster).
	 * @param s The character sequence containing the integer value to parse.
	 * @return The value parsed.
	 * @throws NumberFormatException if the value is malformed or overflows.
	 */
	public static long parseLong(final CharSequence s)
	{
		final int n = s.length();  // Will NPE if necessary.
		if (n == 0)
		{
			throw new NumberFormatException("Empty string");
		}
		char c = s.charAt(0);  // Current character.
		int i = 1;  // index in `s'.
		if (c < '0' && (c == '+' || c == '-'))
		{  // Only 1 test in common case.
			if (n == 1)
			{
				throw new NumberFormatException("Just a sign, no value: " + s);
			}
			else if (n > 20)
			{  // "+9223372036854775807" or "-9223372036854775808"
				throw new NumberFormatException("Value too long: " + s);
			}
			c = s.charAt(1);
			i = 2;  // Skip over the sign.
		}
		else if (n > 19)
		{  // "9223372036854775807"
			throw new NumberFormatException("Value too long: " + s);
		}
		long v = 0;  // The result (negated to easily handle MIN_VALUE).
		do
		{
			if ('0' <= c && c <= '9')
			{
				v -= c - '0';
			}
			else
			{
				throw new NumberFormatException("Invalid character '" + c
						+ "' in " + s);
			}
			if (i == n)
			{
				break;
			}
			v *= 10;
			c = s.charAt(i++);
		}
		while (true);
		if (v > 0)
		{
			throw new NumberFormatException("Overflow in " + s);
		}
		else if (s.charAt(0) == '-')
		{
			return v;  // Value is already negative, return unchanged.
		}
		else if (v == Long.MIN_VALUE)
		{
			throw new NumberFormatException("Overflow in " + s);
		}
		else
		{
			return -v;  // Positive value, need to fix the sign.
		}
	}
}
