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


	private static String[] arrayType = new String[0];

	protected static List<String> splitString(final String s)
	{
		List<String> ret = new ArrayList<String>();
		int len = s.length();
		boolean quoted = false;
		int start = 0;
		char prev = ' ';
		char c = ' ';

		for (int i = 0; i < len; i++)
		{
			c = s.charAt(i);

			//Normal word break
			if (c <= ' ' && !quoted)
			{
				if (i > start)
				{
					ret.add(s.substring(start, i));
				}

				start = i+1;
				prev = c;
				continue;
			}

			//Start of quoted section
			if ((c == '"') && (prev <= ' ') && !quoted)
			{
				quoted = true;
				start = i+1;
				prev = c;
				continue;
			}

			//End of quoted section
			if (quoted && prev == '"' && c <= ' ')
			{
				if (i > start)
				{
					ret.add(s.substring(start, i-1));
				}

				quoted = false;
				start = i+1;
				prev = c;
				continue;
			}

			prev = c;

		}

		if (start != s.length())
		{
			if (quoted && c == '"')
				ret.add(s.substring(start, s.length()-1));
			else
				ret.add(s.substring(start, s.length()));
		}

		return ret;
	}

	protected static String[] oldsplitString(final String s)
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
