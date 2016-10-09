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

package org.kairosdb.core.telnet;

import java.util.HashMap;
import java.util.Map;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 10/7/13
 Time: 11:32 AM
 To change this template use File | Settings | File Templates.
 */
public class TestCommandProvider implements CommandProvider
{
	private Map<String, TelnetCommand> m_commandMap;

	public TestCommandProvider()
	{
		m_commandMap = new HashMap<String, TelnetCommand>();
	}

	@Override
	public TelnetCommand getCommand(String command)
	{
		return (m_commandMap.get(command));
	}

	public void putCommand(String command, TelnetCommand telnetCommand)
	{
		m_commandMap.put(command, telnetCommand);
	}
}
