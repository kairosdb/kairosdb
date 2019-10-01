package org.kairosdb.core.datastore;

import java.util.Date;
import java.util.Objects;

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ServiceKeyValue that = (ServiceKeyValue) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }
}
