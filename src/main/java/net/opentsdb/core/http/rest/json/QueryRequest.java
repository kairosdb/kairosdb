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
package net.opentsdb.core.http.rest.json;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import javax.validation.Valid;
import java.util.Collections;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class QueryRequest
{
	private String startAbsolute;
	private String endAbsolute;
	private int cacheTime;


	@Valid
	private List<Metric> metrics;

	@Valid
	private RelativeTime startRelative;

	@Valid
	private RelativeTime endRelative;

	@JsonCreator
	public QueryRequest(@JsonProperty("start_absolute") String startAbsolute,
			@JsonProperty("start_relative") RelativeTime startRelative,
			@JsonProperty("end_absolute") String endAbsolute,
			@JsonProperty("end_relative") RelativeTime endRelative,
			@JsonProperty("cache_time") int cacheTime,
			@JsonProperty("metrics") List<Metric> metrics)
	{
		this.startAbsolute = startAbsolute;
		this.startRelative = startRelative;
		this.endAbsolute = endAbsolute;
		this.endRelative = endRelative;
		this.cacheTime = cacheTime;

		// todo need to validate that the start and end times make sense. ie. end must be after start, etc.
		checkArgument(startAbsolute != null || startRelative != null);
		this.metrics = checkNotNull(metrics);
	}

	public String getStartAbsolute()
	{
		return startAbsolute;
	}

	public String getEndAbsolute()
	{
		return endAbsolute;
	}

	public int getCacheTime()
	{
		return cacheTime;
	}

	public RelativeTime getStartRelative()
	{
		return startRelative;
	}

	public RelativeTime getEndRelative()
	{
		return endRelative;
	}

	public List<Metric> getMetrics()
	{
		return Collections.unmodifiableList(metrics);
	}
}