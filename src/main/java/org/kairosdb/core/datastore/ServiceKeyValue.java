package org.kairosdb.core.datastore;

import java.util.Date;

public class ServiceKeyValue
{
    private String value;
    private Date lastModified;

    public ServiceKeyValue(String value, Date lastModified)
    {
        this.value = value;
        this.lastModified = lastModified;
    }

    public String getValue()
    {
        return value;
    }

    public Date getLastModified()
    {
        return lastModified;
    }
}
