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

package org.kairosdb.core.datastore;


import org.kairosdb.core.annotation.QueryProperty;
import org.kairosdb.core.annotation.ValidationProperty;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

public class Duration
{
    @Min(1)
    @QueryProperty(
            name = "value",
            label = "Value",
            description = "The number of units for the aggregation buckets",
            default_value = "1",
            validations = {
                    @ValidationProperty(
                            expression = "value > 0",
                            message = "Value must be greater than 0."
                    )
            }
    )
    protected long value;

    @NotNull
    @QueryProperty(
            name = "unit",
            label = "Unit",
            description = "The time unit for the sampling rate",
            default_value = "MILLISECONDS"
    )
    protected TimeUnit unit;

    public Duration()
    {
    }

    public Duration(int value, TimeUnit unit)
    {
        this.value = value;
        this.unit = unit;
    }

    public long getValue()
    {
        return value;
    }

    public TimeUnit getUnit()
    {
        return unit;
    }

    @Override
    public String toString()
    {
        return "Duration{" +
                "value=" + value +
                ", unit=" + unit +
                '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }

        Duration duration = (Duration) o;

        return value == duration.value && unit == duration.unit;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (value ^ (value >>> 32));
        result = 31 * result + (unit != null ? unit.hashCode() : 0);
        return result;
    }
}
