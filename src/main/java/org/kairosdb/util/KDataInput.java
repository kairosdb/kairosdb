package org.kairosdb.util;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 Created by bhawkins on 12/10/13.
 */
public interface KDataInput extends DataInput
{
	static KDataInput createInput(byte[] buf)
	{
		return (new KDataInputStream(new ByteArrayInputStream(buf)));
	}

	static KDataInput createInput(ByteBuffer buf)
	{
		return new ByteBufferDataInput(buf);
	}

	int read(byte[] b) throws IOException;

	void mark();

	void reset() throws IOException;
}
