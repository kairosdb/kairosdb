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
package org.kairosdb.core.groupby;

import org.json.JSONException;
import org.json.JSONWriter;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.annotation.FeatureComponent;
import org.kairosdb.core.annotation.FeatureProperty;
import org.kairosdb.core.annotation.ValidationProperty;
import org.kairosdb.core.formatter.FormatterException;

import javax.validation.constraints.Min;
import java.io.StringWriter;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Groups data points by value. Data points are a range of values specified by range size.
 */
@FeatureComponent(
        name = "value",
        description = "Groups data points by value."
)
public class ValueGroupBy implements GroupBy
{
    @Min(1)
    @FeatureProperty(
            name = "range_size",
            label = "Target Size",
            description = "The range for each value. For example, if the range size is 10, then values between 0-9 are placed in the first group, values between 10-19 into the second group, and so forth.",
            validations = {
                    @ValidationProperty(
                            expression = "value >= 0",
                            message = "Target size must be greater or equal than 0."
                    )
            }
    )
private int rangeSize;

public ValueGroupBy()
        {
        }

public ValueGroupBy(int rangeSize)
        {
        checkArgument(rangeSize>0);

        this.rangeSize=rangeSize;
        }

@Override
public int getGroupId(DataPoint dataPoint,Map<String, String> tags)
        {
        if(dataPoint.isLong())
        return(int)(dataPoint.getLongValue()/rangeSize);
        else if(dataPoint.isDouble())
        return(int)dataPoint.getDoubleValue()/rangeSize;
        else
        return-1;
        }

@Override
public GroupByResult getGroupByResult(final int id)
        {
        return new GroupByResult()
        {
@Override
public String toJson()throws FormatterException
        {
        StringWriter stringWriter=new StringWriter();
        try
        {
        JSONWriter writer=new JSONWriter(stringWriter);

        writer.object();
        writer.key("name").value("value");
        writer.key("range_size").value(rangeSize);

        writer.key("group").object();
        writer.key("group_number").value(id);
        writer.endObject();
        writer.endObject();
        }
        catch(JSONException e)
        {
        throw new FormatterException(e);
        }

        return stringWriter.toString();
        }
        };
        }

@Override
public void setStartDate(long startDate)
        {
        }

public void setRangeSize(int rangeSize)
        {
        this.rangeSize=rangeSize;
        }
        }