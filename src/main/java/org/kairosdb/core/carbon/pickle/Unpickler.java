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

package org.kairosdb.core.carbon.pickle;

import net.razorvine.pickle.Opcodes;

import java.io.IOException;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 10/7/13
 Time: 2:14 PM
 To change this template use File | Settings | File Templates.
 */
public class Unpickler extends net.razorvine.pickle.Unpickler
{
	private boolean m_firstTuple = true;

	@Override
	protected void dispatch(short key) throws IOException
	{
		if (key == Opcodes.TUPLE2)
		{
			if (!m_firstTuple)
			{
				m_firstTuple = true;
				//Pop three items from stack
				Object value = stack.pop();
				long time = ((Number)stack.pop()).longValue();
				String path = (String)stack.pop();

				PickleMetric metric;
				if (value instanceof Double)
					metric = new PickleMetric(path, time, (Double)value);
				else
					metric = new PickleMetric(path, time, ((Number)value).longValue());

				stack.add(metric);
			}
			else
				m_firstTuple = false;
		}
		else
			super.dispatch(key);

	}
}
