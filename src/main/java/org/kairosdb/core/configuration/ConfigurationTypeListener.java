package org.kairosdb.core.configuration;

import com.google.inject.TypeLiteral;
import com.google.inject.spi.TypeEncounter;
import com.google.inject.spi.TypeListener;
import com.typesafe.config.*;
import org.kairosdb.core.KairosRootConfig;
import org.kairosdb.core.annotation.InjectProperty;

import java.lang.reflect.*;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class ConfigurationTypeListener implements TypeListener
{
	private final KairosRootConfig m_config;

	public ConfigurationTypeListener(KairosRootConfig config)
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
					if (m_config.hasPath(prop))
					{
						Parameter[] parameters = method.getParameters();
						//Class<?>[] parameterTypes = method.getParameterTypes();
						if (parameters.length != 1)
						{
							encounter.addError("@InjectProperty method must only take one parameter.");
						}
						else
						{
							Object configValue = getConfigurationValue(parameters[0], prop, encounter);

							if (configValue != null)
								encounter.register(new ConfigMethodInjector<>(method, configValue));

						}
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

	@SuppressWarnings("unchecked")
	private <I> Optional<Object> getConfigurationValue(Parameter parameter,  String path, TypeEncounter<I> encounter)
	{
		Config config = m_config.getRawConfig();
		Class paramClass = parameter.getType();
		Optional<Object> extractedValue = ConfigExtractors.extractConfigValue(config,
				paramClass, path);
		if (extractedValue.isPresent()) {
			return Optional.of(extractedValue.get());
		}

		ConfigValue configValue = config.getValue(path);
		ConfigValueType valueType = configValue.valueType();
		if (valueType.equals(ConfigValueType.OBJECT) && Map.class.isAssignableFrom(paramClass)) {
			ConfigObject object = config.getObject(path);
			return Optional.of(object.unwrapped());
		} else if (valueType.equals(ConfigValueType.OBJECT)) {
			Object bean = ConfigBeanFactory.create(config.getConfig(path), paramClass);
			return Optional.of(bean);
		} else if (valueType.equals(ConfigValueType.LIST) && List.class.isAssignableFrom(paramClass)) {
			Type listType = ((ParameterizedType) parameter.getParameterizedType()).getActualTypeArguments()[0];

			Optional<List<?>> extractedListValue =
					ListExtractors.extractConfigListValue(config, listType, path);

			if (extractedListValue.isPresent()) {
				return Optional.of(extractedListValue.get());
			} else {
				List<? extends Config> configList = config.getConfigList(path);
				return Optional.of(configList.stream()
						.map(cfg -> {
							Object created = ConfigBeanFactory.create(cfg, (Class) listType);
							return created;
						})
						.collect(Collectors.toList()));
			}
		}

		encounter.addError("Cannot obtain config value for " + parameter.getParameterizedType() + " at path: " + path);
		return Optional.empty();
	}
}
