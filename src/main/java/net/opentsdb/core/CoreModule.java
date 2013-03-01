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

package net.opentsdb.core;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import net.opentsdb.core.aggregator.AggregatorFactory;
import net.opentsdb.core.aggregator.GuiceAggregatorFactory;
import net.opentsdb.core.datastore.Datastore;
import net.opentsdb.core.telnet.*;

import java.util.MissingResourceException;
import java.util.Properties;

public class CoreModule extends AbstractModule
{
	public static final String DATASTORE_CLASS_PROPERTY = "opentsdb.datastore.class";
	private Properties m_props;

	public CoreModule(Properties props)
	{
		m_props = props;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected void configure()
	{
		/*String dsClassName = m_props.getProperty(DATASTORE_CLASS_PROPERTY);
		try
		{
			Class dsClass = Class.forName(dsClassName);
			bind(Datastore.class).to(dsClass).in(Scopes.SINGLETON);
		}
		catch (ClassNotFoundException e)
		{
			throw new MissingResourceException("Unable to load Datastore class",
					dsClassName, DATASTORE_CLASS_PROPERTY);
		}*/
		bind(AggregatorFactory.class).to(GuiceAggregatorFactory.class);

		Names.bindProperties(binder(), m_props);
	}
}
