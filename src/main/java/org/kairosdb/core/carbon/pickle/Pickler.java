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
import java.io.OutputStream;
import java.util.List;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 10/7/13
 Time: 10:31 AM
 To change this template use File | Settings | File Templates.
 */
public class Pickler extends net.razorvine.pickle.Pickler
{
	public static final byte PROTOCOL = 2;

	private OutputStream m_out;

	public void writeMetrics(List<PickleMetric> metrics, OutputStream out) throws IOException
	{
		m_out = out;

		dump(metrics, out);
	}

	@Override
	public void save(Object o) throws IOException
	{
		if (o instanceof PickleMetric)
		{
			PickleMetric metric = (PickleMetric) o;
			save(metric.getPath());
			save((double)metric.getTime());
			/*m_out.write(Opcodes.INT);
			m_out.write(String.valueOf(metric.getTime()).getBytes());
			m_out.write('\n');*/
			if (metric.isLongValue())
				save(metric.getLongValue());
			else
				save(metric.getDoubleValue());

			m_out.write(Opcodes.TUPLE2);
			m_out.write(Opcodes.TUPLE2);
		}
		else
			super.save(o);
	}
}
