package org.kairosdb.rollup;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.kairosdb.util.Preconditions.checkNotNullOrEmpty;

/**
 Roll up task.
 */
public class RollupTask
{
	// todo regular expressions
	// todo one time go back and redo option
	// todo setup annotations for validation
	// todo add tags

	private final String id = UUID.randomUUID().toString();
	private final transient List<Rollup> rollups = new ArrayList<Rollup>();
	private String name;
	private String schedule;
	private long timestamp;
	private String json;

	public RollupTask()
	{
	}

	public RollupTask(String name, String schedule, List<Rollup> rollups)
	{
		checkNotNull(rollups);
		checkArgument(rollups.size() > 0);

		this.name = checkNotNullOrEmpty(name);
		this.rollups.addAll(rollups);
		this.schedule = checkNotNullOrEmpty(schedule);
		this.timestamp = System.currentTimeMillis();
	}

	public String getName()
	{
		return name;
	}

	public String getId()
	{
		return id;
	}

	public List<Rollup> getRollups()
	{
		return rollups;
	}

	public void addRollup(Rollup rollup)
	{
		rollups.add(rollup);
	}

	public void addJson(String json)
	{
		checkNotNullOrEmpty(json);
		this.json = json.replaceFirst("\\{", "{\"id\":\"" + id + "\",");
	}

	public String getSchedule()
	{
		return schedule;
	}

	public long getTimestamp()
	{
		return timestamp;
	}

	public String getJson()
	{
		return json;
	}

	@Override
	public boolean equals(Object o)
	{
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		RollupTask that = (RollupTask) o;

		return !(id != null ? !id.equals(that.id) : that.id != null);

	}

	@Override
	public int hashCode()
	{
		return id != null ? id.hashCode() : 0;
	}
}
