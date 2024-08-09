package emissary.server.api;

import emissary.core.EmissaryException;
import emissary.core.Namespace;
import emissary.directory.EmissaryNode;
import emissary.server.EmissaryServer;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.FormParam;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("")
// context is /api and is set in EmissaryServer.java
public class TimeLogging {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @GET
    @Path("/timing")
    @Produces(MediaType.TEXT_HTML)
    public Response turnOnTiming(@Context HttpServletRequest request) {
        return enable(0);
    }

    @GET
    @Path("/timing-{M}")
    @Produces(MediaType.TEXT_HTML)
    public Response turnOnLimitedTiming(@Context HttpServletRequest request, @FormParam("M") int minutes) {
        return enable(minutes);
    }

    @GET
    @Path("/timing-off")
    @Produces(MediaType.TEXT_HTML)
    public Response turnOffTiming(@Context HttpServletRequest request, @FormParam("M") int minutes) {
        return disable();
    }

    private Response enable(int minutes) {
        try {
            EmissaryServer emissaryServer = (EmissaryServer) Namespace.lookup("EmissaryServer");
            EmissaryNode localNode = emissaryServer.getNode();
            boolean success = localNode.setupAgentTimeLogging(minutes);
            if (success) {
                return Response.ok(minutes > 0 ?
                        String.format("Agent timing enabled for %d minutes.", minutes) : "Agent timing enabled.").build();
            }
            return Response.ok("Agent timing already enabled. " +
                    "Please wait for it to conclude before attempting to start again.").build();

        } catch (EmissaryException e) {
            String message = "Exception trying to enable agent timing";
            logger.error(message, e);
            return Response.serverError().entity(message + ": " + e.getMessage()).build();
        }
    }

    private Response disable() {
        try {
            EmissaryServer emissaryServer = (EmissaryServer) Namespace.lookup("EmissaryServer");
            EmissaryNode localNode = emissaryServer.getNode();
            localNode.turnOffAgentTiming();
            return Response.ok("Agent timing disabled.").build();

        } catch (EmissaryException e) {
            String message = "Exception trying to disable agent timing";
            logger.error(message, e);
            return Response.serverError().entity(message + ": " + e.getMessage()).build();
        }
    }

}
