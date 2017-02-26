package org.kairosdb.core.datastore;

import java.util.List;

/**
 Created by bhawkins on 1/28/17.
 */
public interface SearchResult extends QueryCallback
{
	List<DataPointRow> getRows();
}
