package org.kairosdb.core.configuration;

import com.google.inject.TypeLiteral;
import com.google.inject.spi.TypeEncounter;
import com.google.inject.spi.TypeListener;
import org.kairosdb.core.KairosConfig;
import org.kairosdb.core.annotation.InjectProperty;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

public class ConfigurationTypeListener implements TypeListener
{
	private final KairosConfig m_config;

	public ConfigurationTypeListener(KairosConfig config)
	{
		m_config = config;
	}

	@Override
	public <I> void hear(TypeLiteral<I> type, TypeEncounter<I> encounter)
	{
		Class<?> clazz = type.getRawType();
		while (clazz != null)
		{
			for (Field field : clazz.getDeclaredFields())
			{
				if (field.isAnnotationPresent(InjectProperty.class))
				{
					InjectProperty annotation = field.getAnnotation(InjectProperty.class);

					annotation.prop();
					//encounter.register(new Log4JMembersInjector<I>(field));
				}
			}
			for (Method method : clazz.getDeclaredMethods())
			{
				if (method.isAnnotationPresent(InjectProperty.class))
				{
					InjectProperty annotation = method.getAnnotation(InjectProperty.class);

					String prop = annotation.prop();
					if (m_config.getConfig().hasPath(prop))
					{
					}
					else if (!annotation.optional())
					{
						encounter.addError("Configuration not set for '"+prop+"' on method "+method.getName() +" in class "+clazz.getName());
					}

				}
			}
			clazz = clazz.getSuperclass();
		}
	}
}
