/*
 * Copyright 2016 KairosDB Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kairosdb.util;

import java.util.concurrent.ConcurrentHashMap;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 10/16/13
 Time: 9:44 AM
 To change this template use File | Settings | File Templates.
 */
public class StringPool
{
	private ConcurrentHashMap<String, String> m_stringPool;

	public StringPool()
	{
		m_stringPool = new ConcurrentHashMap<String, String>();
	}

	public String getString(String str)
	{
		String ret = m_stringPool.putIfAbsent(str, str);

		return (ret == null) ? str : ret;
	}
}
