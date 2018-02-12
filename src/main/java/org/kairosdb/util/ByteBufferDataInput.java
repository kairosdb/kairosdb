package org.kairosdb.util;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.kairosdb.core.Main.UTF_8;

/**
 Created by bhawkins on 12/19/16.
 */
public class ByteBufferDataInput implements DataInput
{
	private final ByteBuffer m_buffer;

	public ByteBufferDataInput(ByteBuffer buffer)
	{
		m_buffer = buffer;
	}

	@Override
	public void readFully(byte[] b) throws IOException
	{
		m_buffer.get(b);
	}

	@Override
	public void readFully(byte[] b, int off, int len) throws IOException
	{
		m_buffer.get(b, off, len);
	}

	@Override
	public int skipBytes(int n) throws IOException
	{
		int i= 0;
		for (; i < n; i++)
		{
			if (m_buffer.hasRemaining())
				m_buffer.get();
			else
				break;
		}

		return i;
	}

	@Override
	public boolean readBoolean() throws IOException
	{
		return m_buffer.get() != 0;
	}

	@Override
	public byte readByte() throws IOException
	{
		return m_buffer.get();
	}

	@Override
	public int readUnsignedByte() throws IOException
	{
		byte b = m_buffer.get();
		return (b & 0xff);
	}

	@Override
	public short readShort() throws IOException
	{
		return m_buffer.getShort();
	}

	@Override
	public int readUnsignedShort() throws IOException
	{
		return (m_buffer.getShort() & 0xffff);
	}

	@Override
	public char readChar() throws IOException
	{
		return m_buffer.getChar();
	}

	@Override
	public int readInt() throws IOException
	{
		return m_buffer.getInt();
	}

	@Override
	public long readLong() throws IOException
	{
		return m_buffer.getLong();
	}

	@Override
	public float readFloat() throws IOException
	{
		return m_buffer.getFloat();
	}

	@Override
	public double readDouble() throws IOException
	{
		return m_buffer.getDouble();
	}

	@Override
	public String readLine() throws IOException
	{
		return null;
	}

	@Override
	public String readUTF() throws IOException
	{
		return DataInputStream.readUTF(this);
	}
}
