package org.kairosdb.rollup;

import com.google.gson.annotations.SerializedName;
import org.kairosdb.core.groupby.GroupBy;
import org.kairosdb.core.http.rest.json.RelativeTime;
import org.testng.collections.SetMultiMap;

import java.util.List;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.kairosdb.util.Preconditions.checkNotNullOrEmpty;

/**
 * Roll up task.
 */
public class RollUpTask
{
	// todo regular expressions
	// todo one time go back and redo option

	// todo setup annontations for validation

	private final String id;
	@SerializedName("metric_name")
	private final String metricName;
	@SerializedName("start_time")
	private final RelativeTime startTime;
	@SerializedName("end_time")
	private final RelativeTime endTime;
	private final SetMultiMap filter;
	private final List<GroupBy> groupBys;
	private final List<RollupTaskTarget> targets;
	private final String schedule;

	private final RelativeTime backfill; // todo this class is in core.http.rest is this ok?

	private final long timestamp;

	// todo handle grouping

	// todo make back fill optional
	public RollUpTask(String metricName, RelativeTime startTime,
			RelativeTime endTime, SetMultiMap filter, List<GroupBy> groupBys,
			String schedule, List<RollupTaskTarget> targets, RelativeTime backfill)
	{
		checkNotNullOrEmpty(metricName);
		checkNotNullOrEmpty(schedule);
		checkNotNull(backfill);

		this.id = UUID.randomUUID().toString();
		this.metricName = metricName;
		this.schedule = schedule;
		this.startTime = startTime;
		this.endTime = endTime;
		this.filter = filter;
		this.groupBys = groupBys;
		this.targets = targets;
		this.backfill = backfill;

		this.timestamp = System.currentTimeMillis();
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
