package org.kairosdb.core.http.rest.json;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import org.joda.time.DateTimeZone;
import org.kairosdb.core.datastore.PluggableQuery;
import org.kairosdb.core.datastore.QueryMetric;
import org.kairosdb.core.datastore.QueryPlugin;

import javax.validation.Valid;
import javax.validation.constraints.Min;
import java.util.ArrayList;
import java.util.List;

/**
 Created by bhawkins on 5/18/17.
 */
public class Query implements PluggableQuery
{
	@SerializedName("start_absolute")
	private Long m_startAbsolute;

	@SerializedName("end_absolute")
	private Long m_endAbsolute;

	@Min(0)
	@SerializedName("cache_time")
	private int cache_time;

	@Valid
	@SerializedName("start_relative")
	private RelativeTime start_relative;

	@Valid
	@SerializedName("end_relative")
	private RelativeTime end_relative;

	@Valid
	@SerializedName("time_zone")
	private DateTimeZone m_timeZone;// = DateTimeZone.UTC;;

	@Expose(deserialize = false)
	private List<QueryPlugin> m_plugins = new ArrayList<>();

	@Expose(deserialize = false)
	private List<QueryMetric> m_queryMetrics = new ArrayList<>();

	public void setStartAbsolute(Long startAbsolute)
	{
		m_startAbsolute = startAbsolute;
	}

	public void setEndAbsolute(Long endAbsolute)
	{
		m_endAbsolute = endAbsolute;
	}

	public void setCacheTime(int cache_time)
	{
		this.cache_time = cache_time;
	}

	public void setStartRelative(RelativeTime start_relative)
	{
		this.start_relative = start_relative;
	}

	public void setEndRelative(RelativeTime end_relative)
	{
		this.end_relative = end_relative;
	}

	public void setTimeZone(DateTimeZone timeZone)
	{
		m_timeZone = timeZone;
	}

	public void setPlugins(List<QueryPlugin> plugins)
	{
		m_plugins = plugins;
	}

	public void setQueryMetrics(List<QueryMetric> queryMetrics)
	{
		m_queryMetrics = queryMetrics;
	}

	public Long getStartAbsolute()
	{
		return m_startAbsolute;
	}

	public Long getEndAbsolute()
	{
		return m_endAbsolute;
	}

	public int getCacheTime()
	{
		return cache_time;
	}

	public RelativeTime getStartRelative()
	{
		return start_relative;
	}

	public RelativeTime getEndRelative()
	{
		return end_relative;
	}

	public DateTimeZone getTimeZone()
	{
		return m_timeZone;
	}

	public String getCacheString()
	{
		StringBuilder sb = new StringBuilder();
		if (m_startAbsolute != null)
			sb.append(m_startAbsolute).append(":");

		if (start_relative != null)
			sb.append(start_relative.toString()).append(":");

		if (m_endAbsolute != null)
			sb.append(m_endAbsolute).append(":");

		if (end_relative != null)
			sb.append(end_relative.toString()).append(":");

		return (sb.toString());
	}

	@Override
	public String toString()
	{
		return "Query{" +
				"startAbsolute='" + m_startAbsolute + '\'' +
				", endAbsolute='" + m_endAbsolute + '\'' +
				", cache_time=" + cache_time +
				", startRelative=" + start_relative +
				", endRelative=" + end_relative +
				'}';
	}

	@Override
	public List<QueryPlugin> getPlugins()
	{
		return m_plugins;
	}

	@Override
	public void addPlugin(QueryPlugin plugin)
	{
		m_plugins.add(plugin);
	}

	public void addQueryMetric(QueryMetric queryMetric)
	{
		m_queryMetrics.add(queryMetric);
	}

	public List<QueryMetric> getQueryMetrics()
	{
		return m_queryMetrics;
	}
}
