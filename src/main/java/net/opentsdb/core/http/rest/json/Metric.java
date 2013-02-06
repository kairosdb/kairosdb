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