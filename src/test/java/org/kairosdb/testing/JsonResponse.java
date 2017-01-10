/*
 * Copyright 2016 KairosDB Authors
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */


package org.kairosdb.testing;

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