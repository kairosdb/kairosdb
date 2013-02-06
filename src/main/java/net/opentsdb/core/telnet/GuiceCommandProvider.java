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
// see <http://www.gnu.org/licenses/>

package net.opentsdb.core.telnet;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;

public class GuiceCommandProvider implements CommandProvider
{
	private Injector m_injector;

	@Inject
	public GuiceCommandProvider(Injector injector)
	{
		m_injector = injector;
	}

	@Override
	public TelnetCommand getCommand(String command)
	{
		TelnetCommand cmd = m_injector.getInstance(Key.get(TelnetCommand.class, Names.named(command)));

		return (cmd);
	}
}
