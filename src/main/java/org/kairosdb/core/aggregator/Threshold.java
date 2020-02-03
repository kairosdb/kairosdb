package org.kairosdb.core.aggregator;

import org.kairosdb.core.annotation.FeatureProperty;

import javax.annotation.Nonnull;
import java.util.Objects;

class Threshold implements Comparable<Threshold>
{
    enum Boundary
    {
        SUPERIOR,
        INFERIOR
    }

    @FeatureProperty(
            name = "value",
            label = "Value",
            description = "The value of the threshold"
    )
    private double value;

    @FeatureProperty(
            name = "boundary",
            label = "Boundary",
            description = "Determines how to compare against values equal to the threshold value. " +
                    "Values equal to the threshold value are greater than thresholds with inferior boundaries " +
                    "and less than thresholds with superior boundaries.",
            type = "enum",
            options = {"superior", "inferior"},
            default_value = "superior"
    )
    private Boundary boundary = Boundary.SUPERIOR;

    Threshold()
    {
    }

    Threshold(double value, Boundary boundary)
    {
        this.value = value;
        this.boundary = Objects.requireNonNull(boundary, "boundary must not be null");
    }

    public double getValue()
    {
        return value;
    }

    public Boundary getBoundary()
    {
        return boundary;
    }

    int compareValue(double value)
    {
        if (value > this.value)
        {
            return 1;
        }
        if (value < this.value)
        {
            return -1;
        }
        switch (boundary)
        {
            case SUPERIOR:
                return -1;
            case INFERIOR:
                return 1;
            default:
                throw new IllegalStateException("Unknown boundary type: " + boundary);
        }
    }

    @Override
    public String toString()
    {
        return "Threshold{" +
                "value=" + value +
                "boundary=" + boundary +
                '}';
    }

    @Override
    public int compareTo(@Nonnull Threshold threshold)
    {
        if (equals(threshold))
        {
            return 0;
        }

        return threshold.compareValue(value);
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
        Threshold threshold = (Threshold) o;
        return Double.compare(threshold.value, value) == 0 &&
                boundary == threshold.boundary;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(value, boundary);
    }
}
