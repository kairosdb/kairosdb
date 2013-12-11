package org.kairosdb.util;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 Created by bhawkins on 12/10/13.
 */
public class KDataOutput implements DataOutput
{
	private ByteArrayOutputStream m_arrayOutputStream;
	private DataOutputStream m_dataOutputStream;

	public KDataOutput()
	{
		m_arrayOutputStream = new ByteArrayOutputStream();
		m_dataOutputStream = new DataOutputStream(m_arrayOutputStream);
	}

	public byte[] getBytes() throws IOException
	{
		m_dataOutputStream.flush();
		return m_arrayOutputStream.toByteArray();
	}

	@Override
	public void write(int b) throws IOException
	{
		m_dataOutputStream.write(b);
	}

	@Override
	public void write(byte[] b) throws IOException
	{
		m_dataOutputStream.write(b);
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException
	{
		m_dataOutputStream.write(b, off, len);
	}

	@Override
	public void writeBoolean(boolean v) throws IOException
	{
		m_dataOutputStream.writeBoolean(v);
	}

	@Override
	public void writeByte(int v) throws IOException
	{
		m_dataOutputStream.writeByte(v);
	}

	@Override
	public void writeShort(int v) throws IOException
	{
		m_dataOutputStream.writeShort(v);
	}

	@Override
	public void writeChar(int v) throws IOException
	{
		m_dataOutputStream.writeChar(v);
	}

	@Override
	public void writeInt(int v) throws IOException
	{
		m_dataOutputStream.writeInt(v);
	}

	@Override
	public void writeLong(long v) throws IOException
	{
		m_dataOutputStream.writeLong(v);
	}

	@Override
	public void writeFloat(float v) throws IOException
	{
		m_dataOutputStream.writeFloat(v);
	}

	@Override
	public void writeDouble(double v) throws IOException
	{
		m_dataOutputStream.writeDouble(v);
	}

	@Override
	public void writeBytes(String s) throws IOException
	{
		m_dataOutputStream.writeBytes(s);
	}

	@Override
	public void writeChars(String s) throws IOException
	{
		m_dataOutputStream.writeChars(s);
	}

	@Override
	public void writeUTF(String s) throws IOException
	{
		m_dataOutputStream.writeUTF(s);
	}
}
