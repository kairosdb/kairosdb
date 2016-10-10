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

package org.kairosdb.core.telnet;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneDecoder;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

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
		return splitString(((ChannelBuffer) msg).toString(CHARSET));
	}


	protected static String[] splitString(final String s)
	{
		final char[] chars = s.trim().toCharArray();
		int num_substrings = 0;
		boolean last_was_space = true;
		for (final char x : chars)
		{
			if (Character.isWhitespace(x))
			    last_was_space = true;
			else
			{
				if (last_was_space)
					num_substrings++;
				last_was_space = false;
			}
		}

		final String[] result = new String[num_substrings];
		final int len = chars.length;
		int start = 0;  // starting index in chars of the current substring.
		int pos = 0;    // current index in chars.
		int i = 0;      // number of the current substring.
		last_was_space = true;
		for (; pos < len; pos++)
		{
			if (Character.isWhitespace(chars[pos]))
			{
				if (!last_was_space)
					result[i++] = new String(chars, start, pos - start);
				last_was_space = true;
			}
			else
			{
				if (last_was_space)
					start = pos;
				last_was_space = false;
			}
		}
		if (!last_was_space)
			result[i] = new String(chars, start, pos - start);
		return result;
	}
}
