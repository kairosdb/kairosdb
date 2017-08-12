/*
 * Copyright 2016 KairosDB Authors
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


import com.google.common.collect.ImmutableList;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.kairosdb.core.aggregator.Sampling;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class Util
{
	/**
	 Special thanks to Nadeau software consulting for publishing this code.
	 http://nadeausoftware.com/node/97
	 @param s string representation of number to parse
	 @return number
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

	/**
	 * Returns the host name. First tries to execute "hostname" on the machine. This should work for Linux, Windows,
	 * and Mac. If, for some reason hostname fails, then get the name from InetAddress (which might change depending
	 * on network setup)
	 *
	 * @return hostname
	 */
	public static String getHostName()
	{
		try
		{
			Runtime run = Runtime.getRuntime();
			Process process = run.exec("hostname");
			try(BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream())))
			{
				// Need to read all lines from the stream or the process could hang
				StringBuilder buffer = new StringBuilder();
				String line;
				while ((line = br.readLine()) != null)
					buffer.append(line);
	
				int returnValue = process.waitFor();
				if (returnValue == 0)
					return buffer.toString();
			}
		}
		catch (Exception e)
		{
			// ignore
		}

		try
		{
			return InetAddress.getLocalHost().getHostName();
		}
		catch (UnknownHostException e)
		{
			return "";
		}
	}

	public static void packUnsignedLong(long value, DataOutput buffer) throws IOException
	{
		/* Encodes a value using the variable-length encoding from
		<a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html">
		Google Protocol Buffers</a>. Zig-zag is not used, so input must not be negative.
		If values can be negative, use {@link #writeSignedVarLong(long, DataOutput)}
		instead. This method treats negative input as like a large unsigned value. */
		while ((value & ~0x7FL) != 0L)
		{
			buffer.writeByte((int) ((value & 0x7F) | 0x80));
			value >>>= 7;
		}
		buffer.writeByte((int) value);
	}

	public static long unpackUnsignedLong(DataInput buffer) throws IOException
	{
		int shift = 0;
		long result = 0;
		while (shift < 64)
		{
			final byte b = buffer.readByte();
			result |= (long)(b & 0x7F) << shift;
			if ((b & 0x80) == 0)
			{
				return result;
			}
			shift += 7;
		}
		throw new IllegalArgumentException("Variable length quantity is too long");
	}

	public static void packLong(long value, DataOutput buffer) throws IOException
	{
		// Great trick from http://code.google.com/apis/protocolbuffers/docs/encoding.html#types
		packUnsignedLong((value << 1) ^ (value >> 63), buffer);

	}

	public static long unpackLong(DataInput buffer) throws IOException
	{
		long value = unpackUnsignedLong(buffer);

		return ((value >>> 1) ^ -(value & 1));
	}

	public static InetAddress findPublicIp()
	{
		// Check if local host address is a good v4 address
		InetAddress localAddress = null;
		try {
			localAddress = InetAddress.getLocalHost();
			if (isGoodV4Address(localAddress)) {
				return localAddress;
			}
		}
		catch (UnknownHostException ignored) {
		}
		if (localAddress == null) {
			try {
				localAddress = InetAddress.getByAddress(new byte[]{127, 0, 0, 1});
			}
			catch (UnknownHostException e) {
				throw new AssertionError("Could not get local ip address");
			}
		}

		// check all up network interfaces for a good v4 address
		for (NetworkInterface networkInterface : getGoodNetworkInterfaces()) {
			for (InetAddress address : Collections.list(networkInterface.getInetAddresses())) {
				if (isGoodV4Address(address)) {
					return address;
				}
			}
		}
		// check all up network interfaces for a good v6 address
		for (NetworkInterface networkInterface : getGoodNetworkInterfaces()) {
			for (InetAddress address : Collections.list(networkInterface.getInetAddresses())) {
				if (isGoodV6Address(address)) {
					return address;
				}
			}
		}
		// just return the local host address
		// it is most likely that this is a disconnected developer machine
		return localAddress;
	}

	/**
	 Returns true if the string contains a number. This means it contains only digits, the minus sign, plus sign
	 and a period.

	 @param s string to test
	 @return true if only contains a number value
	 */
	public static boolean isNumber(String s)
	{
		checkNotNull(s);

		if (s.isEmpty())
			return false;

		int start = 0;
		char firstChar = s.charAt(0);
		if (firstChar == '+' || firstChar == '-' || firstChar == '.')
		{
			start = 1;
			if (s.length() == 1)
				return false;
		}

		for (int i = start; i < s.length(); i++)
		{
			char c = s.charAt(i);
			if (!Character.isDigit(c) && c != '.')
				return false;
		}

		//noinspection RedundantIfStatement
		if (s.charAt(s.length() - 1) == '.')
			return false; // can't have trailing period

		return true;
	}

	private static List<NetworkInterface> getGoodNetworkInterfaces()
	{
		ImmutableList.Builder<NetworkInterface> builder = ImmutableList.builder();
		try {
			for (NetworkInterface networkInterface : Collections.list(NetworkInterface.getNetworkInterfaces())) {
				try {
					if (!networkInterface.isLoopback() && networkInterface.isUp()) {
						builder.add(networkInterface);
					}
				}
				catch (Exception ignored) {
				}
			}
		}
		catch (SocketException ignored) {
		}
		return builder.build();
	}

	private static boolean isGoodV4Address(InetAddress address)
	{
		return address instanceof Inet4Address &&
				!address.isAnyLocalAddress() &&
				!address.isLoopbackAddress() &&
				!address.isMulticastAddress();
	}

	private static boolean isGoodV6Address(InetAddress address)
	{
		return address instanceof Inet6Address &&
				!address.isAnyLocalAddress() &&
				!address.isLoopbackAddress() &&
				!address.isMulticastAddress();
	}


	/**
	 Computes the duration of the sampling (value * unit) starting at timestamp.

	 @param timestamp unix timestamp of the start time.
	 @return the duration of the sampling in millisecond.
	 */
	public static long getSamplingDuration(long timestamp, Sampling sampling, DateTimeZone timeZone)
	{
		long ret = sampling.getValue();
		DateTime dt = new DateTime(timestamp, timeZone);
		switch (sampling.getUnit())
		{
			case YEARS:
				ret = new org.joda.time.Duration(dt, dt.plusYears((int) sampling.getValue())).getMillis();
				break;
			case MONTHS:
				ret = new org.joda.time.Duration(dt, dt.plusMonths((int) sampling.getValue())).getMillis();
				break;
			case WEEKS:
				ret = new org.joda.time.Duration(dt, dt.plusWeeks((int) sampling.getValue())).getMillis();
				break;
			case DAYS:
				ret = new org.joda.time.Duration(dt, dt.plusDays((int) sampling.getValue())).getMillis();
				break;
			case HOURS:
				ret = new org.joda.time.Duration(dt, dt.plusHours((int) sampling.getValue())).getMillis();
				break;
			case MINUTES:
				ret = new org.joda.time.Duration(dt, dt.plusMinutes((int) sampling.getValue())).getMillis();
				break;
			case SECONDS:
				ret = new org.joda.time.Duration(dt, dt.plusSeconds((int) sampling.getValue())).getMillis();
				break;
			case MILLISECONDS:
				ret = sampling.getValue();
				break;
		}
		return ret;
	}
}
