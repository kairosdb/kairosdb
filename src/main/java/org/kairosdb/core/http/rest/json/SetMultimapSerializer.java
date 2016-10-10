package org.kairosdb.core.http.rest.json;

import com.google.common.collect.SetMultimap;
import com.google.gson.JsonElement;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;

public class SetMultimapSerializer implements JsonSerializer<SetMultimap>
{
	@Override
	public JsonElement serialize(SetMultimap src, Type typeOfSrc, JsonSerializationContext context)
	{
		return context.serialize(src.asMap());
	}
}
