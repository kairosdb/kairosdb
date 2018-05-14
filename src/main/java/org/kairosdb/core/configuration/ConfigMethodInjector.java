package org.kairosdb.core.configuration;

import com.google.inject.MembersInjector;
import com.google.inject.spi.InjectionListener;
import com.typesafe.config.Config;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class ConfigMethodInjector<T> implements MembersInjector<T>
{

	private final Object m_value;
	private final Method m_method;

	public ConfigMethodInjector(Method method, Object value)
	{
		m_method = method;
		m_value = value;
	}

	@Override
	public void injectMembers(T instance)
	{
		try
		{
			m_method.invoke(instance, m_value);
		}
		catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}
}
