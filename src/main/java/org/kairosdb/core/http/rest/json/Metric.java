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

import org.apache.bval.constraints.NotEmpty;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class Metric
{
	@NotNull
	@NotEmpty()
	private String name;

	@NotNull
	@NotEmpty()
	private String aggregate;

	@Valid
	private Sampling sampling;

	private boolean rate;
	private Map<String, String> tags;
	private String groupBy;

	@JsonCreator
	public Metric(@JsonProperty("name") String name,
	              @JsonProperty("aggregate") String aggregate,
	              @JsonProperty("sampling") Sampling sampling,
	              @JsonProperty("rate") boolean rate,
	              @JsonProperty("tags") Map<String, String> tags,
	              @JsonProperty("group_by") String groupBy
	)
	{
		this.name = name;
		this.aggregate = aggregate;
		this.sampling = sampling;
		this.rate = rate;
		this.tags = tags;
		this.groupBy = groupBy;
	}

	public String getName()
	{
		return name;
	}

	public String getAggregate()
	{
		return aggregate;
	}

	public Sampling getSampling()
	{
		return sampling;
	}

	public boolean isRate()
	{
		return rate;
	}

	public Map<String, String> getTags()
	{
		if (tags != null)
		{
			return new HashMap<String, String>(tags);
		}
		else
		{
			return Collections.emptyMap();
		}
	}

	public String getGroupBy()
	{
		return groupBy;
	}
}