// KairosDB2
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
package org.kairosdb.core.http.rest.json;

import org.apache.bval.constraints.NotEmpty;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonDeserialize;

import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class NewMetricRequest
{
	@NotNull
	@NotEmpty
	private String name;

	private Map<String, String> tags;

	@Valid
	@JsonDeserialize(using = DataPointDeserializer.class)
	private List<DataPointRequest> datapoints = new ArrayList<DataPointRequest>();

	@JsonCreator
	public NewMetricRequest(@JsonProperty("name") String name,
			@JsonProperty("tags") Map<String, String> tags)
	{
		this.name = name;
		this.tags = tags;
	}

	@JsonProperty
	public String getName()
	{
		return name;
	}

	public void addDataPoint(DataPointRequest dataPoint)
	{
		this.datapoints.add(dataPoint);
	}

	public List<DataPointRequest> getDatapoints()
	{
		return Collections.unmodifiableList(datapoints);
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