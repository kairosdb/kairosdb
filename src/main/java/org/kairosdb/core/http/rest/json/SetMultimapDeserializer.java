package org.kairosdb.core.http.rest.json;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.gson.*;

import java.lang.reflect.Type;
import java.util.Map;

public class SetMultimapDeserializer implements JsonDeserializer<SetMultimap<String, String>>
{
	@Override
	public SetMultimap<String, String> deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException
	{
		SetMultimap<String, String> map = HashMultimap.create();

		JsonObject filters = json.getAsJsonObject();
		for (Map.Entry<String, JsonElement> filter : filters.entrySet())
		{
			String name = filter.getKey();
			JsonArray values = ((JsonArray)filter.getValue());
			for (JsonElement value : values)
			{
				map.put(name, value.getAsString());
			}
		}

		return map;
	}
}
