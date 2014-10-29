package org.kairosdb.core.http.rest.json;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;

import java.lang.reflect.Type;

public class RelativeTimeDeserializer implements JsonDeserializer<RelativeTime>
{
	@Override
	public RelativeTime deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException
	{
		return null;
	}
}
