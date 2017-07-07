package org.kairosdb.util;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.nio.ByteBuffer;

/**
 Created by bhawkins on 12/10/13.
 */
public class KDataInput
{
	public static DataInput createInput(byte[] buf)
	{
		return (new DataInputStream(new ByteArrayInputStream(buf)));
	}

	public static DataInput createInput(ByteBuffer buf)
	{
		return new ByteBufferDataInput(buf);
	}
}
