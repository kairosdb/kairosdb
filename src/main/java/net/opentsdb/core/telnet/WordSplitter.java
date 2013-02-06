// OpenTSDB2
// Copyright (C) 2013 Proofpoint, Inc.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>

package net.opentsdb.core.telnet;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneDecoder;

import java.nio.charset.Charset;

public class WordSplitter extends OneToOneDecoder
{
	private static final Charset CHARSET = Charset.forName("ISO-8859-1");

	/**
	 Constructor.
	 */
	public WordSplitter()
	{
	}

	@Override
	protected Object decode(final ChannelHandlerContext ctx,
			final Channel channel,
			final Object msg) throws Exception
	{
		return splitString(((ChannelBuffer) msg).toString(CHARSET), ' ');
	}

	private String[] splitString(final String s, final char c)
	{
		final char[] chars = s.toCharArray();
		int num_substrings = 1;
		for (final char x : chars)
		{
			if (x == c)
			{
				num_substrings++;
			}
		}

		final String[] result = new String[num_substrings];
		final int len = chars.length;
		int start = 0;  // starting index in chars of the current substring.
		int pos = 0;    // current index in chars.
		int i = 0;      // number of the current substring.
		for (; pos < len; pos++)
		{
			if (chars[pos] == c)
			{
				result[i++] = new String(chars, start, pos - start);
				start = pos + 1;
			}
		}
		result[i] = new String(chars, start, pos - start);
		return result;
	}
}
