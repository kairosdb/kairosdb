package org.kairosdb.rollup;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.gson.annotations.SerializedName;
import org.kairosdb.core.groupby.GroupBy;
import org.kairosdb.core.http.rest.json.RelativeTime;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.kairosdb.util.Preconditions.checkNotNullOrEmpty;

/**
 * Roll up task.
 */
public class RollUpTask
{
	// todo regular expressions
	// todo one time go back and redo option
	// todo setup annotations for validation

	@SerializedName("metric_name")
	private String metricName;

	@SerializedName("start_relative")
	private RelativeTime startTime;

	@SerializedName("end_relative")
	private RelativeTime endTime;

	@SerializedName("group_bys")
	private final SetMultimap<String, String> filters = HashMultimap.create();

	private String schedule;

	private final String id = UUID.randomUUID().toString();
	private final List<GroupBy> groupBys = new ArrayList<GroupBy>();
	private final List<RollupTaskTarget> targets = new ArrayList<RollupTaskTarget>();
	private long timestamp;


	private RelativeTime backfill; // todo this class is in core.http.rest is this ok?

	public RollUpTask()
	{
	}

	public RollUpTask(String metricName, RelativeTime startTime,
			List<RollupTaskTarget> targets, String schedule)
	{
		checkNotNullOrEmpty(metricName);
		checkNotNull(startTime);
		checkNotNull(targets);
		checkArgument(targets.size() > 0);
		checkNotNullOrEmpty(schedule);

		this.metricName = metricName;
		this.startTime = startTime;
		this.targets.addAll(targets);
		this.schedule = schedule;
		this.timestamp = System.currentTimeMillis();
	}

	public RollUpTask setEndTime(RelativeTime time)
	{
		checkNotNull(time);
		this.endTime = time;
		return this;
	}

	public RollUpTask addFilter(String name, String value)
	{
		checkNotNullOrEmpty(name);
		checkNotNullOrEmpty(value);

		filters.put(name, value);
		return this;
	}

	public RollUpTask addGroupBy(GroupBy groupBy)
	{
		checkNotNull(groupBy);

		groupBys.add(groupBy);
		return this;
	}

	public void setBackfill(RelativeTime backfill)
	{
		checkNotNull(backfill);

		this.backfill = backfill;
	}

	public String getId()
	{
		return id;
	}

	public String getMetricName()
	{
		return metricName;
	}

	public String getSchedule()
	{
		return schedule;
	}

	public long getTimestamp()
	{
		return timestamp;
	}

	public RelativeTime getBackfill()
	{
		return backfill;
	}

	public RelativeTime getStartTime()
	{
		return startTime;
	}

	public SetMultimap<String, String> getFilters()
	{
		return filters;
	}

	public List<GroupBy> getGroupBys()
	{
		return groupBys;
	}

	public List<RollupTaskTarget> getTargets()
	{
		return targets;
	}

	public RelativeTime getEndTime()
	{
		return endTime;
	}

	@Override
	public boolean equals(Object o)
	{
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		RollUpTask that = (RollUpTask) o;

		if (id != null ? !id.equals(that.id) : that.id != null) return false;

		return true;
	}

	@Override
	public int hashCode()
	{
		return id != null ? id.hashCode() : 0;
	}
}
