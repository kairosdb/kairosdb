===========
Custom Data
===========

KairosDB provides a means for storing and aggregating custom data types.  By default KairosDB supports long, double and string values.

-----------------------------------
Steps for creating custom data type
-----------------------------------

1.  Create a plugin_.
2.  Create a DataPointFactory implementation.
3.  Create a DataPoint implementation.
4.  Bind your DataPointFactory in your plugin module.
5.  Register custom type in the properties file.

Look at StringDataPointFactory.java and StringDataPoint.java for examples of how to implement.

---------------------------------
Example for creating custom types
---------------------------------

For this example we want a custom type for complex numbers that have a real and imaginary part.  Following is the code for the DataPoint and DataPointFactory implementations:

ComplexDataPoint
::
	package org.kairosdb.core.datapoints;

	import org.json.JSONException;
	import org.json.JSONWriter;

	import java.io.DataOutput;
	import java.io.IOException;

	/**
	 Used to show how to create a custom data type
	 Created by bhawkins on 6/27/14.
	 */
	public class ComplexDataPoint extends DataPointHelper
	{
		private static final String API_TYPE = "complex";
		private double m_real;
		private double m_imaginary;

		public ComplexDataPoint(long timestamp, double real, double imaginary)
		{
			super(timestamp);
			m_real = real;
			m_imaginary = imaginary;
		}

		@Override
		public void writeValueToBuffer(DataOutput buffer) throws IOException
		{
			buffer.writeDouble(m_real);
			buffer.writeDouble(m_imaginary);
		}

		@Override
		public void writeValueToJson(JSONWriter writer) throws JSONException
		{
			writer.object();

			writer.key("real").value(m_real);
			writer.key("imaginary").value(m_imaginary);

			writer.endObject();
		}

		@Override
		public String getApiDataType()
		{
			return API_TYPE;
		}

		@Override
		public String getDataStoreDataType()
		{
			return ComplexDataPointFactory.DST_COMPLEX;
		}

		@Override
		public boolean isLong()
		{
			return false;
		}

		@Override
		public long getLongValue()
		{
			return 0;
		}

		@Override
		public boolean isDouble()
		{
			return false;
		}

		@Override
		public double getDoubleValue()
		{
			return 0;
		}
	}

ComplexDataPointFactory
::
	package org.kairosdb.core.datapoints;

	import com.google.gson.JsonElement;
	import com.google.gson.JsonObject;
	import org.kairosdb.core.DataPoint;

	import java.io.DataInput;
	import java.io.IOException;

	/**
	 Used to show how to create a custom data type
	 Created by bhawkins on 6/30/14.
	 */
	public class ComplexDataPointFactory implements DataPointFactory
	{
		public static final String DST_COMPLEX = "kairos_complex";
		public static final String GROUP_TYPE = "complex";

		@Override
		public String getDataStoreType()
		{
			return DST_COMPLEX;
		}

		@Override
		public String getGroupType()
		{
			return GROUP_TYPE;
		}

		@Override
		public DataPoint getDataPoint(long timestamp, JsonElement json) throws IOException
		{
			if (json.isJsonObject())
			{
				JsonObject object = json.getAsJsonObject();
				double real = object.get("real").getAsDouble();
				double imaginary = object.get("imaginary").getAsDouble();

				return new ComplexDataPoint(timestamp, real, imaginary);
			}
			else
				throw new IOException("JSON object is not a valid complex data point");
		}

		@Override
		public DataPoint getDataPoint(long timestamp, DataInput buffer) throws IOException
		{
			double real = buffer.readDouble();
			double imaginary = buffer.readDouble();

			return new ComplexDataPoint(timestamp, real, imaginary);
		}
	}

Inside our plugin module we'll need to bind the ComplexDataPointFactory like so:
::
	bind(ComplexDataPointFactory.class).in(Singleton.class);
	
Inside our plugin properties file we'll need to register our api type:
::
	kairosdb.datapoints.factory.complex=org.kairosdb.core.datapoints.ComplexDataPointFactory
	
So why are the above two steps separate and required?  The first step binds our factory into guice and registers the datastore type of 'kairos_complex'.  The second step registers the api type.  Lets say down the road we change how we want to store the complex type.  Without this separation the only way to change is by exporting all the data in importing it using the new method.  With this separation I can register a new factory that defines the datastore type as 'kairos_complex2' and bind it to the 'complex' api type.  New data will now be stored in the new format and yet the system will still be able to read the old data.

.. _plugin: Plugins.html
