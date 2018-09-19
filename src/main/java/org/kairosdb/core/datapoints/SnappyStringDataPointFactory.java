package org.kairosdb.core.datapoints;

import com.google.gson.JsonElement;
import org.kairosdb.core.DataPoint;
import org.kairosdb.util.KDataInput;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;

/**
 Created by bhawkins on 09/15/2018.
 */
public class SnappyStringDataPointFactory implements DataPointFactory
{
	public static final String DST_STRING = "kairos_string";
	public static final String GROUP_TYPE = "text";
	public static final Charset UTF8 = Charset.forName("UTF-8");

	@Override
	public String getDataStoreType()
	{
		return DST_STRING;
	}

	@Override
	public String getGroupType()
	{
		return GROUP_TYPE;
	}

	@Override
	public DataPoint getDataPoint(long timestamp, JsonElement json) throws IOException
	{
		StringDataPoint ret = new StringDataPoint(timestamp, json.getAsString());
		return ret;
	}

	@Override
	public DataPoint getDataPoint(long timestamp, KDataInput buffer) throws IOException
	{
		int buffSz = buffer.readUnsignedShort();

		byte[] byteBuffer = new byte[buffSz];

		buffer.readFully(byteBuffer, 0, buffSz);

		String result;
		if (Snappy.isValidCompressedBuffer(byteBuffer, 0, buffSz))
		{
			byte[] uncompressedArray = new byte[Snappy.uncompressedLength(byteBuffer, 0, buffSz)];
			int incompressedLength = Snappy.uncompress(byteBuffer, 0, buffSz, uncompressedArray, 0);

			result = new String(uncompressedArray, 0, incompressedLength, UTF8);
		}
		else
		{
			result = new String(byteBuffer, UTF8);
		}

		SnappyStringDataPoint ret = new SnappyStringDataPoint(timestamp, result);
		return ret;
	}

	public DataPoint createDataPoint(long timestamp, String value)
	{
		SnappyStringDataPoint ret = new SnappyStringDataPoint(timestamp, value);
		return ret;
	}
}

