package org.kairosdb.core.http.rest;

import org.kairosdb.core.datastore.MetaDatastore;
import org.kairosdb.core.exception.DatastoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.print.attribute.standard.Media;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.kairosdb.core.http.rest.MetricsResource.setHeaders;

/**
 Created by bhawkins on 12/19/15.
 */
@Path("/api/v1/meta")
public class MetadataResource
{
	public static final Logger logger = LoggerFactory.getLogger(MetadataResource.class);
	private final MetaDatastore m_metastore;


	@Inject
	public MetadataResource(MetaDatastore metastore)
	{
		m_metastore = metastore;
	}



	@GET
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("/")
	public List<String> getNamespaces() throws DatastoreException
	{
		List<String> ret = new ArrayList<>();

		Iterable<String> namespaces = m_metastore.getNamespaces();
		for (String namespace : namespaces)
		{
			ret.add(namespace);
		}

		return ret;
	}


	@GET
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("/{namespace}")
	public List<String> getMetaKeys(@PathParam("namespace")String namespace) throws DatastoreException
	{
		List<String> ret = new ArrayList<String>();

		Iterable<String> keys = m_metastore.getKeys(namespace);
		for (String key : keys)
		{
			ret.add(key);
		}

		return ret;
	}


	@GET
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("/{namespace}/{metaKey}")
	public String getMetaValue(@PathParam("namespace")String namespace, @PathParam("metaKey")String metaKey) throws DatastoreException
	{
		return m_metastore.getValue(namespace, metaKey);
	}


	@POST
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("/{namespace}/{metaKey}")
	public Response setMetaValue(@PathParam("namespace")String namespace,
			@PathParam("metaKey")String metaKey, String value) throws DatastoreException
	{
		m_metastore.setValue(namespace, metaKey, value);

		return setHeaders(Response.status(Response.Status.NO_CONTENT)).build();
	}


	@DELETE
	@Produces(MediaType.APPLICATION_JSON + "; charset=UTF-8")
	@Path("/{namespace}/{metaKey}")
	public Response deleteMetaValue(@PathParam("namespace")String namespace,
			@PathParam("metaKey")String metaKey) throws DatastoreException
	{
		m_metastore.deleteValue(namespace, metaKey);

		return setHeaders(Response.status(Response.Status.NO_CONTENT)).build();
	}
}
