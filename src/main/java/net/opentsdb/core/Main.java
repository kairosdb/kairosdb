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

import com.google.inject.*;
import jcmdline.*;
import net.opentsdb.core.http.WebServletModule;
import net.opentsdb.core.telnet.TelnetServer;
import org.eclipse.jetty.server.Server;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

public class Main
{
	private static FileParam s_propertiesFile = new FileParam("p",
			"a custom properties file", FileParam.IS_FILE & FileParam.IS_READABLE,
			FileParam.OPTIONAL);


	public static void main(String[] args) throws Exception
	{
		CmdLineHandler cl = new VersionCmdLineHandler("Version 2.0",
				new HelpCmdLineHandler("Opentsdb Help", "opentsdb", "Starts OpenTSDB",
						new Parameter[] { s_propertiesFile }, null));

		cl.parse(args);
		Main main = new Main();

		Injector injector = main.createGuiceInjector();

		Map<Key<?>, Binding<?>> bindings =
				injector.getAllBindings();

		/*System.out.println("Checking bindings");
		for (Key<?> key : bindings.keySet())
		{
			Class bindingClass = key.getTypeLiteral().getRawType();
			Set<Class> interfaces = new HashSet<Class>(Arrays.asList(bindingClass.getInterfaces()));
			if (interfaces.contains(ProtocolService.class))
				System.out.println(bindingClass);
		}*/

		startTelnetListener(injector);
		startWebServer(injector);
	}

	public Injector createGuiceInjector() throws IOException
	{
		Properties props = new Properties();
		props.load(getClass().getClassLoader().getResourceAsStream("opentsdb.properties"));

		if (s_propertiesFile.isSet())
			props.load(new FileInputStream(s_propertiesFile.getValue()));

		Injector injector = Guice.createInjector(new CoreModule(props),
				new WebServletModule());

		return (injector);
	}

	public static void startTelnetListener(Injector injector)
	{
		TelnetServer ts = injector.getInstance(TelnetServer.class);
		//MyTelnetServer ts = injector.getInstance(MyTelnetServer.class);

		ts.run();
		System.out.println("Done run");
	}

	public static void startWebServer(Injector injector) throws Exception
	{
		Server server = injector.getInstance(Server.class);

		System.out.println("Starting web server");
		server.start();
		server.join();
	}

}
