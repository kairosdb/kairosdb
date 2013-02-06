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

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.Collections;
import java.util.Map;

public class NewMetricRequest
{
	@NotNull
	@NotEmpty
	private String name;

	@NotNull
	@NotEmpty
	private String value;

	@Min(1)
	private long timestamp;

	private Map<String, String> tags;


	@JsonCreator
	public NewMetricRequest(@JsonProperty("name") String name,
			@JsonProperty("value") String value,
			@JsonProperty("timestamp") long timestamp,
			@JsonProperty("tags") Map<String, String> tags)
	{
		this.name = name;
		this.value = value;
		this.timestamp = timestamp;
		this.tags = tags;
	}

	@JsonProperty
	public String getName()
	{
		return name;
	}

	@JsonProperty
	public String getValue()
	{
		return value;
	}

	@JsonProperty
	public long getTimestamp()
	{
		return timestamp;
	}

	@JsonProperty
	public Map<String, String> getTags()
	{
		if (tags != null)
		{
			return Collections.unmodifiableMap(tags);
		}
		else
		{
			return Collections.unmodifiableMap(Collections.<String, String>emptyMap());
		}
	}
}