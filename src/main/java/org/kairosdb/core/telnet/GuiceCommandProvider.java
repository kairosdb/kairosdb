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

package org.kairosdb.core.telnet;

import com.google.inject.Binding;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;

import java.util.HashMap;
import java.util.Map;

public class GuiceCommandProvider implements CommandProvider
{
	private Map<String, TelnetCommand> m_commandMap = new HashMap<String, TelnetCommand>();

	@Inject
	public GuiceCommandProvider(Injector injector)
	{
		Map<Key<?>, Binding<?>> bindings = injector.getAllBindings();

		for (Key<?> key : bindings.keySet())
		{
			Class<?> bindingClass = key.getTypeLiteral().getRawType();
			if (TelnetCommand.class.isAssignableFrom(bindingClass))
			{
				TelnetCommand command = (TelnetCommand)injector.getInstance(bindingClass);
				m_commandMap.put(command.getCommand(), command);
			}
		}
	}

	@Override
	public TelnetCommand getCommand(String command)
	{
		TelnetCommand cmd = m_commandMap.get(command);

		return (cmd);
	}
}
