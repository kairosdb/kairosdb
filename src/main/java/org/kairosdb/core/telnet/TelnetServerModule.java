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

package org.kairosdb.core.telnet;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.google.inject.name.Names;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class TelnetServerModule extends AbstractModule
{
	public static final Logger logger = LoggerFactory.getLogger(TelnetServerModule.class);

	private Properties m_props;


	public TelnetServerModule(Properties props)
	{
		m_props = props;
	}

	@Override
	protected void configure()
	{
		logger.info("Configuring module TelnetServerModule");

		bind(TelnetServer.class).in(Singleton.class);
		bind(PutCommand.class).in(Singleton.class);
		bind(VersionCommand.class).in(Singleton.class);
		bind(CommandProvider.class).to(GuiceCommandProvider.class);
	}
}
