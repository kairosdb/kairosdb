package org.kairosdb.core.http.rest.json;

import com.google.gson.*;

import java.lang.reflect.Type;

public class RelativeTimeSerializer implements JsonSerializer<RelativeTime>
{
	@Override
	public JsonElement serialize(RelativeTime relativeTime, Type type, JsonSerializationContext jsonSerializationContext)
	{
		JsonObject jsonObject = new JsonObject();
		jsonObject.add("value", new JsonPrimitive(relativeTime.getValue()));
		jsonObject.add("unit", new JsonPrimitive(relativeTime.getUnit().name().toLowerCase()));

		return jsonObject;
	}
}
