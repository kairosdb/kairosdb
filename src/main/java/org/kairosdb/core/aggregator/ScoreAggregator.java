package org.kairosdb.core.aggregator;

import org.kairosdb.core.DataPoint;
import org.kairosdb.core.annotation.FeatureComponent;
import org.kairosdb.core.annotation.FeatureProperty;
import org.kairosdb.core.datapoints.DoubleDataPointFactory;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.core.groupby.GroupByResult;
import org.kairosdb.plugin.Aggregator;

import javax.inject.Inject;
import javax.validation.constraints.NotNull;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;

@FeatureComponent(
        name = "score",
        label = "SCORE",
        description = "Scores the data based on a set of thresholds. Each data point will be mapped to a value between 0 and n where n is the number of thresholds."
)
public class ScoreAggregator implements Aggregator
{
    enum Order
    {
        ASCENDING,
        DESCENDING
    }

    private final DoubleDataPointFactory dataPointFactory;

    @NotNull
    @FeatureProperty(
            name = "thresholds",
            label = "Thresholds",
            description = "A set of thresholds"
    )
    private Threshold[] thresholds;

    @NotNull
    @FeatureProperty(
            name = "order",
            label = "Order",
            description = "The order by which scores are assigned",
            type = "enum",
            options = {"ascending", "descending"},
            default_value = "ascending"
    )
    private Order order = Order.ASCENDING;

    @Inject
    public ScoreAggregator(DoubleDataPointFactory dataPointFactory)
    {
        this.dataPointFactory = dataPointFactory;
    }

    public void setThresholds(Threshold[] thresholds)
    {
        this.thresholds = thresholds;
        Arrays.sort(thresholds);
    }

    public Threshold[] getThresholds()
    {
        return thresholds;
    }

    public void setOrder(Order order)
    {
        this.order = order;
    }

    public Order getOrder()
    {
        return order;
    }

    public boolean canAggregate(String groupType)
    {
        return DataPoint.GROUP_NUMBER.equals(groupType);
    }

    public String getAggregatedGroupType(String s)
    {
        return dataPointFactory.getGroupType();
    }

    public DataPointGroup aggregate(DataPointGroup dataPointGroup)
    {
        Objects.requireNonNull(dataPointGroup);

        return new MappedDataPointGroup(dataPointGroup);
    }

    private class MappedDataPointGroup implements DataPointGroup
    {
        private DataPointGroup innerDataPointGroup;

        MappedDataPointGroup(DataPointGroup innerDataPointGroup)
        {
            this.innerDataPointGroup = innerDataPointGroup;
        }

        @Override
        public boolean hasNext()
        {
            return (innerDataPointGroup.hasNext());
        }

        @Override
        public DataPoint next()
        {
            DataPoint dp = innerDataPointGroup.next();

            int score;
            for (score = 0; score < thresholds.length; score++)
            {
                if (thresholds[score].compareValue(dp.getDoubleValue()) < 0)
                {
                    break;
                }
            }

            if (order == Order.DESCENDING)
            {
                score = thresholds.length - score;
            }

            return dataPointFactory.createDataPoint(dp.getTimestamp(), score);
        }

        @Override
        public void remove()
        {
            innerDataPointGroup.remove();
        }

        @Override
        public String getName()
        {
            return (innerDataPointGroup.getName());
        }

        @Override
        public List<GroupByResult> getGroupByResult()
        {
            return (innerDataPointGroup.getGroupByResult());
        }

        @Override
        public void close()
        {
            innerDataPointGroup.close();
        }

        @Override
        public Set<String> getTagNames()
        {
            return (innerDataPointGroup.getTagNames());
        }

        @Override
        public Set<String> getTagValues(String tag)
        {
            return (innerDataPointGroup.getTagValues(tag));
        }
    }
}
