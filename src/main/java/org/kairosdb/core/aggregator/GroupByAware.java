package org.kairosdb.core.aggregator;

import org.kairosdb.plugin.GroupBy;

import java.util.List;

/**
 Created by bhawkins on 2/9/16.
 */
public interface GroupByAware
{
	void setGroupBys(List<GroupBy> groupBys);
}
