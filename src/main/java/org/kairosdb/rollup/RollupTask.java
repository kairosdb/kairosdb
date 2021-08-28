package org.kairosdb.rollup;

import com.google.gson.annotations.SerializedName;
import org.apache.bval.constraints.NotEmpty;
import org.kairosdb.core.datastore.Duration;

import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.kairosdb.util.Preconditions.requireNonNullOrEmpty;

/**
 Roll up task.
 */
public class RollupTask
{
	// todo regular expressions
	// todo one time go back and redo option
	// todo setup annotations for validation
	// todo add tags

	private final String id;
	private final transient List<Rollup> rollups = new ArrayList<>();

	@NotNull
	@NotEmpty()
	private String name;

	@NotNull
	@SerializedName("execution_interval")
	private Duration executionInterval;

	private long lastModified;
	private String json;

	public RollupTask()
	{
		id = UUID.randomUUID().toString();
	}

	public RollupTask(String name, Duration executionInterval, List<Rollup> rollups)
	{
		requireNonNull(rollups);
		checkArgument(rollups.size() > 0);

		id = UUID.randomUUID().toString();
		initialize(name, executionInterval, rollups);
	}

	public RollupTask(String id, String name, Duration executionInterval, List<Rollup> rollups, String json)
	{
		requireNonNullOrEmpty(id);
		requireNonNullOrEmpty(json);
		requireNonNull(rollups);
		checkArgument(rollups.size() > 0);

		this.id = id;
		this.json = json;
		initialize(name, executionInterval, rollups);
	}

	private void initialize(String name, Duration executionInterval, List<Rollup> rollups)
	{
		this.name = requireNonNullOrEmpty(name);
		this.rollups.addAll(rollups);
		this.executionInterval = requireNonNull(executionInterval);
		this.lastModified = System.currentTimeMillis();
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
		requireNonNullOrEmpty(json);

		if (json.contains("\"id\":"))
		{
			// if id already exist in the json replace it
			this.json = json.replaceFirst("\"id\":\".\",", "\"id\":\" + id + \"");
		}
		else
		{
			// if not add it
			this.json = json.replaceFirst("\\{", "{\"id\":\"" + id + "\",");
		}
	}

	public Duration getExecutionInterval()
	{
		return executionInterval;
	}

	public long getLastModified()
	{
		return lastModified;
	}

	public void setLastModified(long lastModified)
	{
		this.lastModified = lastModified;
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
