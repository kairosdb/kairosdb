// OpenTSDB2
// Copyright (C) 2013 Proofpoint, Inc.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>package net.opentsdb.testing;


package net.opentsdb.testing;

import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class JsonResponse
{
	private String json;
	private String statusString;
	private int statusCode;
	private Map<String, String> headers = new HashMap<String, String>();

	public JsonResponse(HttpResponse response) throws IOException
	{
		Header[] allHeaders = response.getAllHeaders();
		for (Header header : allHeaders)
		{
			headers.put(header.getName(), header.getValue());
		}

		statusString = response.getStatusLine().getReasonPhrase();
		statusCode = response.getStatusLine().getStatusCode();
		HttpEntity entity = response.getEntity();

		if (entity != null)
			json = CharStreams.toString(new InputStreamReader(entity.getContent(), Charsets.UTF_8));
	}

	public String getJson()
	{
		return json;
	}

	public String getStatusString()
	{
		return statusString;
	}

	public int getStatusCode()
	{
		return statusCode;
	}

	public String getHeader(String name)
	{
		return headers.get(name);
	}
}