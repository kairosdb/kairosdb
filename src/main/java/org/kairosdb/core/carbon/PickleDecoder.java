/*
 * Copyright 2013 Proofpoint Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kairosdb.core.carbon;

import net.razorvine.pickle.Unpickler;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneDecoder;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 10/2/13
 Time: 12:06 PM
 To change this template use File | Settings | File Templates.
 */
public class PickleDecoder extends OneToOneDecoder
{
	private Unpickler m_unpickler = new Unpickler();

	public PickleDecoder()
	{
	}

	@Override
	protected Object decode(ChannelHandlerContext channelHandlerContext,
			Channel channel, Object o) throws Exception
	{
		ChannelBuffer cb = (ChannelBuffer)o;

		Unpickler unpickler = new Unpickler();

		return (unpickler.load(new ChannelBufferInputStream(cb)));
	}
}
