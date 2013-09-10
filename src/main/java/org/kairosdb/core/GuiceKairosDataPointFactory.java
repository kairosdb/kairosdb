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

package org.kairosdb.core;

import com.google.inject.Binding;
import com.google.inject.Injector;
import com.google.inject.Key;
import org.kairosdb.core.aggregator.Aggregator;
import org.kairosdb.core.aggregator.annotation.AggregatorName;

import javax.inject.Inject;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 9/10/13
 Time: 10:25 AM
 To change this template use File | Settings | File Templates.
 */
public class GuiceKairosDataPointFactory implements KairosDataPointFactory
{

	@Inject
	public GuiceKairosDataPointFactory(Injector injector)
	{
		m_injector = injector;
		Map<Key<?>, Binding<?>> bindings = injector.getAllBindings();

		for (Key<?> key : bindings.keySet())
		{
			Class bindingClass = key.getTypeLiteral().getRawType();
			if (Aggregator.class.isAssignableFrom(bindingClass))
			{
				AggregatorName ann = (AggregatorName)bindingClass.getAnnotation(AggregatorName.class);
				if (ann == null)
					throw new IllegalStateException("Aggregator class "+bindingClass.getName()+
							" does not have required annotation "+AggregatorName.class.getName());

				m_aggregators.put(ann.name(), bindingClass);
			}
		}
	}

	@Override
	public DataPoint createDataPoint(String type, long timestamp, String json)
	{
		return null;  //To change body of implemented methods use File | Settings | File Templates.
	}

	@Override
	public DataPoint createDataPoint(String type, long timestamp, ByteBuffer buffer)
	{
		return null;  //To change body of implemented methods use File | Settings | File Templates.
	}

	@Override
	public DataPoint createDataPoint(byte type, long timestamp, ByteBuffer buffer)
	{
		return null;  //To change body of implemented methods use File | Settings | File Templates.
	}

	@Override
	public byte getTypeByte(String type)
	{
		return 0;  //To change body of implemented methods use File | Settings | File Templates.
	}
}
