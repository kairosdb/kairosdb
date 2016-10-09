/*
 * Copyright 2016 KairosDB Authors
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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.bval.constraints.NotEmpty;

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