package org.kairosdb.core.formatter;

import org.kairosdb.core.datastore.QueryResults;

import java.io.IOException;
import java.io.Writer;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 7/1/13
 Time: 10:04 AM
 To change this template use File | Settings | File Templates.
 */
public class JsonResponse
{
	private Writer m_writer;

	public JsonResponse(Writer writer)
	{
		m_writer = writer;
	}


	void begin() throws FormatterException
	{
	}


	void startQueries() throws FormatterException
	{
	}


	void formatQuery(QueryResults queryResults) throws FormatterException
	{
	}


	void endQueries() throws FormatterException
	{
	}


	public void writeMeta() throws FormatterException
	{
	}


	void end() throws FormatterException
	{

	}

}
