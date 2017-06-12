package org.kairosdb.core.http.rest;

import org.kairosdb.core.http.rest.json.Query;

/**
 Created by bhawkins on 6/2/17.
 */
public interface PreProcessor
{
	Query preProcessQuery(Query query);
}
