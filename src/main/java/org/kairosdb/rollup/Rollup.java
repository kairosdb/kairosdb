package org.kairosdb.rollup;

import com.google.gson.annotations.SerializedName;
import org.apache.bval.constraints.NotEmpty;
import org.kairosdb.core.datastore.QueryMetric;

import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.List;

public class Rollup
{
	@NotNull
	@NotEmpty()
	@SerializedName("save_as")
	private String saveAs;

	private final transient List<QueryMetric> queryMetrics = new ArrayList<QueryMetric>();
	// todo add tags

	//	public Rollup(String saveAs, QueryMetric query)
	//	{
	//		// todo add checks for null and empty
	//		this.saveAs = saveAs;
	//		this.query = query;
	//	}

	public String getSaveAs()
	{
		return saveAs;
	}

	public List<QueryMetric> getQueryMetrics()
	{
		return queryMetrics;
	}

	public void addQueries(List<QueryMetric> queries)
	{
		this.queryMetrics.addAll(queries);
	}

	public void addQuery(QueryMetric query)
	{
		queryMetrics.add(query);
	}
}
