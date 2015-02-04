package gr.iti.vcl.crawl.rest;

import gr.iti.vcl.crawl.impl.CrawlerImpl;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.Consumes;
import javax.ws.rs.Path;
import javax.ws.rs.POST;
import javax.ws.rs.Produces;
import org.codehaus.jettison.json.JSONObject;

/**
 * REST Web Service
 *
 * @author Michalis Lazaridis <lazar@iti.gr>
 */
@Path("crawl")
public class CrawlerRest {

    @Context
    private UriInfo context;
    private static CrawlerImpl impl = new CrawlerImpl();

    /**
     * Creates a new instance of CrawlerRest
     */
    public CrawlerRest() {
    }

    @POST
    @Consumes("application/json")
    @Produces("application/json")
    public JSONObject postJson(JSONObject content) {
        
        return impl.postService(content);
    }
}
