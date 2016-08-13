/*
 * Copyright 2013 Proofpoint Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.kairosdb.core.http.rest.json;

import org.apache.bval.jsr303.ApacheValidationProvider;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import org.kairosdb.core.http.rest.BeanValidationException;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class DataPointDeserializer extends JsonDeserializer<List<DataPointRequest>>
{
	private static final Validator VALIDATOR = Validation.byProvider(ApacheValidationProvider.class).configure().buildValidatorFactory().getValidator();

	@Override
	public List<DataPointRequest> deserialize(JsonParser parser, DeserializationContext deserializationContext) throws IOException
{
		List<DataPointRequest> datapoints = new ArrayList<DataPointRequest>();

		JsonToken token = parser.nextToken();
		if (token != JsonToken.START_ARRAY )
			throw deserializationContext.mappingException("Invalid data point syntax.");

	while(token != null && token != JsonToken.END_ARRAY)
		{
		 	parser.nextToken();
			long timestamp = parser.getLongValue();

			parser.nextToken();
			String value = parser.getText();

			DataPointRequest dataPointRequest = new DataPointRequest(timestamp, value);

			validateObject(dataPointRequest);
			datapoints.add(dataPointRequest);

			token = parser.nextToken();
			if (token != JsonToken.END_ARRAY)
				throw deserializationContext.mappingException("Invalid data point syntax.");

			token = parser.nextToken();
		}

		return datapoints;
	}

	private void validateObject(Object request) throws BeanValidationException
	{
		// validate object using the bean validation framework
		Set<ConstraintViolation<Object>> violations = VALIDATOR.validate(request);
		if (!violations.isEmpty()) {
			throw new BeanValidationException(violations);
		}
	}
}

