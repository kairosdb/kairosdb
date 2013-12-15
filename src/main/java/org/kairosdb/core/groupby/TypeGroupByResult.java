/*
 * Copyright 2013 Proofpoint Inc.
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
package org.kairosdb.core.groupby;

import org.json.JSONException;
import org.json.JSONWriter;
import org.kairosdb.core.formatter.FormatterException;

import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class TypeGroupByResult implements GroupByResult
{
	private final String m_type;

	public TypeGroupByResult(String type)
	{
		m_type = type;
	}

	public String getType()
	{
		return m_type;
	}

	@Override
	public String toJson() throws FormatterException
	{
		StringWriter stringWriter = new StringWriter();
		JSONWriter writer = new JSONWriter(stringWriter);

		try
		{
			writer.object();
			writer.key("name").value("type");
			writer.key("type").value(m_type);
			writer.endObject();
		}
		catch (JSONException e)
		{
			throw new FormatterException(e);
		}
		return stringWriter.toString();
	}
}