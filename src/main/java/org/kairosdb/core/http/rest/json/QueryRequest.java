/*
 * Copyright 2013 Proofpoint Inc.
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

package org.kairosdb.core.http.rest.json;

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