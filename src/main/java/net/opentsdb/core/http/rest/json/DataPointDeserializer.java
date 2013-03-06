//
//  DataPointDeserializer.java
//
// Copyright 2013, Proofpoint Inc. All rights reserved.
//        
package net.opentsdb.core.http.rest.json;

import net.opentsdb.core.http.rest.BeanValidationException;
import org.apache.bval.jsr303.ApacheValidationProvider;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.JsonDeserializer;

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

