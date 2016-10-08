package org.kairosdb.util;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;

/**
 Created by bhawkins on 3/13/15.
 */
public class PluginClassLoader extends URLClassLoader
{
	ClassLoader m_parentLoader;

	public PluginClassLoader(URL[] urls, ClassLoader parent)
	{
		super(urls, parent);
		m_parentLoader = parent;
	}

	@Override
	protected Class<?> loadClass(String name, boolean resolve)
			throws ClassNotFoundException
	{
		synchronized (getClassLoadingLock(name))
		{
			// First, check if the class has already been loaded
			Class c = findLoadedClass(name);

			if (c == null)
			{
				// If still not found, then invoke findClass in order
				// to find the class.
				try
				{
					c = findClass(name);
				}
				catch (ClassNotFoundException e)
				{
					//pass to the parent to throw exception
				}
			}

			if (c == null)
			{
				c = m_parentLoader.loadClass(name);
			}
			if (resolve)
			{
				resolveClass(c);
			}
			return c;
		}
	}

	@Override
	public URL getResource(String name)
	{
		URL url;

		url = findResource(name);

		if (url == null)
			url = m_parentLoader.getResource(name);

		return url;
	}


	@Override
	public Enumeration<URL> getResources(String name) throws IOException
	{
		@SuppressWarnings("unchecked")
		Enumeration<URL>[] tmp = new Enumeration[2];
		tmp[0] = findResources(name);
		tmp[1] = m_parentLoader.getResources(name);

		return new CompoundEnumeration<URL>(tmp);
	}

}
