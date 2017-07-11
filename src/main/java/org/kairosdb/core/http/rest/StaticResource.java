package org.kairosdb.core.http.rest;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.kairosdb.core.http.WebServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

@Path("/")
public class StaticResource
{
    private final static String responseTemplate = "<html>" +
            "   <head>" +
            "       <meta http-equiv=\"Content-Type\" content=\"text/html;charset=ISO-8859-1\"/>" +
            "       <title>Error {err_num} {err_msg}</title>" +
            "   </head>" +
            "   <body>" +
            "       <h2>HTTP ERROR: {err_num}</h2><p>Problem accessing /{err_target}. Reason:<pre>    {err_msg}</pre></p>" +
            "       <hr/><i><small>Powered by KairosDB://</small></i>" +
            "   </body>" +
            "</html>";
    private final static Logger logger = LoggerFactory.getLogger(StaticResource.class);
    private java.nio.file.Path webroot;

    @Inject
    public StaticResource(@Named(WebServer.JETTY_WEB_ROOT_PROPERTY) String webroot)
    {
        try
        {
            this.webroot = Paths.get(new File(".").getCanonicalPath(), webroot);

        } catch (IOException e)
        {
            logger.error("Impossible to get webroot or to access static resources.", e);
        }
    }

    @GET
    public Response getRoot() throws IOException
    {
        return loadFiles("index.html");
    }

    @GET
    @Path("{res : .*}")
    public Response getResource(@PathParam("res") String resourceName) throws IOException
    {
        return loadFiles(resourceName);
    }

    private Response loadFiles(String pathFile) throws IOException
    {
        final File file = new File(String.valueOf(webroot.resolve(pathFile)));

        if (!file.exists())
            return formatError(Response.Status.NOT_FOUND, pathFile).build();
        if (!file.isFile() || !file.canRead())
            return formatError(Response.Status.FORBIDDEN, pathFile).build();

        try (FileInputStream fis = new FileInputStream(file))
        {
            final String mediaType = Files.probeContentType(file.toPath());
            final Response.ResponseBuilder response = Response.ok(Files.readAllBytes(file.toPath()), mediaType);

            return response.build();
        }
    }

    private Response.ResponseBuilder formatError(Response.Status status, String target)
    {
        final String message = responseTemplate
                .replaceAll("\\{err_num\\}", String.valueOf(status.getStatusCode()))
                .replaceAll("\\{err_msg\\}", status.getReasonPhrase())
                .replaceAll("\\{err_target\\}", target);
        return Response.status(status).entity(message);
    }
}
