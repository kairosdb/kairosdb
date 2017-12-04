package org.kairosdb.plugin;

import org.kairosdb.core.http.rest.json.Query;

/**
 Created by bhawkins on 6/2/17.
 */
public interface QueryPreProcessor
{
	Query preProcessQuery(Query query);
}
