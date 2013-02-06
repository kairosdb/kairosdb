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


import net.opentsdb.core.datastore.TimeUnit;
import net.opentsdb.core.http.rest.validation.TimeUnitRequired;
import org.apache.bval.constraints.NotEmpty;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

public class Sampling
{
	@Min(1)
	private int duration;

	@TimeUnitRequired
	private String unit;

	@NotNull
	@NotEmpty()
	private String aggregate;

	@JsonCreator
	public Sampling(@JsonProperty("duration") int duration,
	                @JsonProperty("unit") String unit,
	                @JsonProperty("aggregate") String aggregate)
	{
		this.duration = duration;
		this.unit = unit;
		this.aggregate = aggregate;
	}

	public int getDuration()
	{
		return duration;
	}

	public TimeUnit getUnit()
	{
		return TimeUnit.from(unit);
	}

	public String getAggregate()
	{
		return aggregate;
	}
}