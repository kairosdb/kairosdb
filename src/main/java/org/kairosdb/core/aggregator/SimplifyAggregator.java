package org.kairosdb.core.aggregator;

import com.goebl.simplify.PointExtractor;
import com.goebl.simplify.Simplify;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.UnmodifiableIterator;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.aggregator.annotation.AggregatorName;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.core.groupby.GroupByResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 Created by jrussek on 11.02.14.
 */
@AggregatorName(name = "simplify", description = "reduces the number of datapoints without altering the graph appearance.")
public class SimplifyAggregator implements Aggregator
{
	public static final Logger logger = LoggerFactory.getLogger(SimplifyAggregator.class);

	private static final String SIMPLIFY_WINDOW = "kairosdb.aggregator.simplify.window";
	private Double tolerance;
	private boolean highquality = false;
	private int maxpoints;
	private static Simplify<DataPoint> simplify = new Simplify<DataPoint>(new DataPoint[0], new SimplifyPointExtractor());

	@Inject
	public SimplifyAggregator(@Named(SIMPLIFY_WINDOW) int maxpoints)
	{
		logger.debug("using a maximum simplify window of " + maxpoints);
		this.maxpoints = maxpoints;
	}

	@SuppressWarnings("unused")
	public void setTolerance(Double tolerance)
	{
		this.tolerance = tolerance;
	}

	@SuppressWarnings("unused")
	public void setHighquality(boolean highquality)
	{
		this.highquality = highquality;
	}

	@Override
	public DataPointGroup aggregate(DataPointGroup dataPointGroup)
	{
		checkNotNull(dataPointGroup);
		return new SimplifiedDataPointGroup(dataPointGroup);
	}

	@Override
	public boolean canAggregate(String groupType)
	{
		return DataPoint.GROUP_NUMBER.equals(groupType);
	}

	private class SimplifiedDataPointGroup implements DataPointGroup
	{
		private DataPointGroup innerDataPointGroup;
		private UnmodifiableIterator<DataPoint> workingSetIterator;

		public SimplifiedDataPointGroup(DataPointGroup input)
		{
			this.innerDataPointGroup = input;
		}

		@Override
		public String getName()
		{
			return this.innerDataPointGroup.getName();
		}

		@Override
		public List<GroupByResult> getGroupByResult()
		{
			return this.innerDataPointGroup.getGroupByResult();
		}

		@Override
		public void close()
		{
			this.innerDataPointGroup.close();
		}

		@Override
		public boolean hasNext()
		{
			return this.workingSetIterator != null && this.workingSetIterator.hasNext() || this.innerDataPointGroup.hasNext();
		}

		@Override
		public DataPoint next()
		{
			if (this.workingSetIterator != null && this.workingSetIterator.hasNext())
			{
				return this.workingSetIterator.next();
			}
			else
			{
				if (this.innerDataPointGroup.hasNext())
				{
					List<DataPoint> datapoints = Lists.newArrayList();
					for (int i = 0; this.innerDataPointGroup.hasNext() && (i < maxpoints); i++)
					{
						datapoints.add(this.innerDataPointGroup.next());
					}
					if (datapoints.size() > 1)
					{
						DataPoint[] result = simplify.simplify(datapoints.toArray(new DataPoint[datapoints.size()]), tolerance, highquality);
						this.workingSetIterator = Iterators.forArray(result);
						logger.debug("simplified " + datapoints.size() + " datapoints down to " + result.length + " datapoints");
					}
					else
					{
						this.workingSetIterator = Iterators.unmodifiableIterator(datapoints.iterator());
						logger.debug("skipping simplification of " + datapoints.size() + " datapoints");
					}
					return this.workingSetIterator.next();
				}
				else
				{
					logger.warn("something went horribly wrong and we will now return a null");
					return null; // should never get here
				}
			}
		}

		@Override
		public void remove()
		{
			this.innerDataPointGroup.remove();
		}

		@Override
		public Set<String> getTagNames()
		{
			return this.innerDataPointGroup.getTagNames();
		}

		@Override
		public Set<String> getTagValues(String tag)
		{
			return this.innerDataPointGroup.getTagNames();
		}
	}


	public static class SimplifyPointExtractor implements PointExtractor<DataPoint>
	{
		@Override
		public double getX(DataPoint dataPoint) {
			return dataPoint.getTimestamp();
		}

		@Override
		public double getY(DataPoint dataPoint) {
			return dataPoint.getDoubleValue();
		}
	}
}
