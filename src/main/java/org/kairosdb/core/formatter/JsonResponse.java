package org.kairosdb.core.formatter;

import org.json.JSONException;
import org.json.JSONWriter;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.core.groupby.GroupByResult;

import java.io.IOException;
import java.io.Writer;
import java.util.List;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 7/1/13
 Time: 10:04 AM
 To change this template use File | Settings | File Templates.
 */
public class JsonResponse
{
	private Writer m_writer;
	private JSONWriter m_jsonWriter;
	private boolean m_began = false;
	private boolean m_beganQueries = false;

	public JsonResponse(Writer writer)
	{
		m_writer = writer;
		m_jsonWriter = new JSONWriter(writer);
	}


	public void begin() throws FormatterException
	{
		try
		{
			m_jsonWriter.object();
			m_began = true;
		}
		catch (JSONException e)
		{
			throw new FormatterException(e);
		}
	}


	public void startQueries() throws FormatterException
	{
		try
		{
			m_jsonWriter.key("queries").array();
			m_beganQueries = true;
		}
		catch (JSONException e)
		{
			throw new FormatterException(e);
		}

	}


	public void formatQuery(List<DataPointGroup> queryResults) throws FormatterException
	{
		try
		{
			m_jsonWriter.object().key("results").array();

			for (DataPointGroup group : queryResults)
			{
				final String metric = group.getName();

				m_jsonWriter.object();
				m_jsonWriter.key("name").value(metric);

				if (!group.getGroupByResult().isEmpty())
				{
					m_jsonWriter.key("group_by");
					m_jsonWriter.array();
					boolean first = true;
					for (GroupByResult groupByResult : group.getGroupByResult())
					{
						if (!first)
							m_writer.write(",");
						m_writer.write(groupByResult.toJson());
						first = false;
					}
					m_jsonWriter.endArray();
				}

				m_jsonWriter.key("tags").object();

				for (String tagName : group.getTagNames())
				{
					m_jsonWriter.key(tagName);
					m_jsonWriter.value(group.getTagValues(tagName));
				}
				m_jsonWriter.endObject();

				m_jsonWriter.key("values").array();
				while (group.hasNext())
				{
					DataPoint dataPoint = group.next();

					m_jsonWriter.array().value(dataPoint.getTimestamp());
					if (dataPoint.isInteger())
					{
						m_jsonWriter.value(dataPoint.getLongValue());
					}
					else
					{
						final double value = dataPoint.getDoubleValue();
						if (value != value || Double.isInfinite(value))
						{
							throw new IllegalStateException("NaN or Infinity:" + value + " data point=" + dataPoint);
						}
						m_jsonWriter.value(value);
					}
					m_jsonWriter.endArray();
				}
				m_jsonWriter.endArray();
				m_jsonWriter.endObject();
			}

			m_jsonWriter.endArray().endObject();
		}
		catch (JSONException e)
		{
			throw new FormatterException(e);
		}
		catch (IOException e)
		{
			throw new FormatterException(e);
		}
	}


	public void endQueries() throws FormatterException
	{
		try
		{
			m_jsonWriter.endArray();
			m_beganQueries = false;
		}
		catch (JSONException e)
		{
			throw new FormatterException(e);
		}

	}


	public void writeMeta() throws FormatterException
	{
	}


	public void end() throws FormatterException
	{
		try
		{
			m_jsonWriter.endObject();
		}
		catch (JSONException e)
		{
			throw new FormatterException(e);
		}
	}

}
