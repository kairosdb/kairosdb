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
package net.opentsdb.core.formatter;

import com.chargebee.org.json.JSONException;
import com.chargebee.org.json.JSONWriter;
import net.opentsdb.core.DataPoint;
import net.opentsdb.core.datastore.DataPointGroup;
import net.opentsdb.core.datastore.StringIterable;

import java.io.Writer;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class JsonFormatter implements DataFormatter
{
	@Override
	public void format(Writer writer, Iterable<String> iterable) throws FormatterException
	{
		checkNotNull(writer);
		checkNotNull(iterable);

		try
		{
			JSONWriter jsonWriter = new JSONWriter(writer);
			jsonWriter.object().key("results").array();
			for (String string : iterable)
			{
				jsonWriter.value(string);
			}
			jsonWriter.endArray().endObject();
		} catch (JSONException e)
		{
			throw new FormatterException(e);
		}
	}

	@Override
	public void format(Writer writer, List<List<DataPointGroup>> data) throws FormatterException
	{

		checkNotNull(writer);
		checkNotNull(data);
		try
		{
			JSONWriter jsonWriter = new JSONWriter(writer);

			jsonWriter.object().key("queries").array();

			for (List<DataPointGroup> groups : data)
			{
				jsonWriter.object().key("results").array();

				for (DataPointGroup group : groups)
				{
					final String metric = group.getName();

					jsonWriter.object();
					jsonWriter.key("name").value(metric);
					jsonWriter.key("tags").object();

					for (String tagName : group.getTags().keySet())
					{
						jsonWriter.key(tagName);
						jsonWriter.value(group.getTags().get(tagName));
					}
					jsonWriter.endObject();

					jsonWriter.key("values").array();
					while (group.hasNext())
					{
						DataPoint dataPoint = group.next();

						jsonWriter.array().value(dataPoint.getTimestamp());
						if (dataPoint.isInteger())
						{
							jsonWriter.value(dataPoint.getLongValue());
						}
						else
						{
							final double value = dataPoint.getDoubleValue();
							if (value != value || Double.isInfinite(value))
							{
								throw new IllegalStateException("NaN or Infinity:" + value + " data point=" + dataPoint);
							}
							jsonWriter.value(value);
						}
						jsonWriter.endArray();
					}
					jsonWriter.endArray();
					jsonWriter.endObject();

					group.close();
				}

				jsonWriter.endArray().endObject();
			}

			jsonWriter.endArray().endObject();
		} catch (JSONException e)
		{
			throw new FormatterException(e);
		}
	}
}