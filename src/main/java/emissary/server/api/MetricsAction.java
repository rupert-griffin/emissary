package emissary.server.api;

import emissary.core.MetricsManager;
import emissary.core.NamespaceException;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("")
// context is /api, set in EmissaryServer
public class MetricsAction {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @GET
    @Path("/metrics")
    @Produces(MediaType.APPLICATION_JSON)
    public Response clusterAgents() {
        try {

            return Response.ok().entity(MetricsManager.lookup().getMetricRegistry()).build();
        } catch (NamespaceException ex) {
            logger.warn("Could not lookup MetricsManager", ex);
            return Response.serverError().entity("Could not lookup MetricsManager").build();
        }
    }
}
