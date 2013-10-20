package org.kairosdb.core.datastore;

import org.kairosdb.core.http.rest.BeanValidationException;
import org.kairosdb.core.http.rest.json.GsonParser;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 10/20/13
 Time: 6:51 AM
 To change this template use File | Settings | File Templates.
 */
public enum Order
{
	ASC("asc"),
	DESC("desc");

	private String m_text;

	Order(String text)
	{
		m_text = text;
	}

	public String getText()
	{
		return (m_text);
	}


	public static Order fromString(String text, String context) throws BeanValidationException
	{
		Order ret = null;

		if (text != null)
		{
			for (Order o : Order.values())
			{
				if (text.equalsIgnoreCase(o.m_text))
				{
					ret = o;
					break;
				}
			}
		}


		if (ret == null)
		{
			throw new BeanValidationException(new GsonParser.SimpleConstraintViolation("order", "must be either 'asc' or 'desc'"), context);
		}
		else
			return (ret);
	}
}
