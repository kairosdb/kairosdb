package org.kairosdb.core.http.rest;

import org.kairosdb.core.http.rest.json.Query;

/**
 Created by bhawkins on 6/12/17.
 */
public interface QueryPreProcessorContainer
{
	Query preProcess(Query query);
}
