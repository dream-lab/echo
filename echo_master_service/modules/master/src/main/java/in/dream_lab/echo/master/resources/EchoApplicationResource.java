package in.dream_lab.echo.master.resources;

import com.codahale.metrics.annotation.Timed;
import in.dream_lab.echo.master.AppManager;
import in.dream_lab.echo.master.ResourceDirectoryClientFactory;
import in.dream_lab.echo.master.db.EchoApplication;
import in.dream_lab.echo.utils.DataflowInput;

import javax.validation.Valid;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Created by pushkar on 8/11/17.
 */
@Path("/DAG")
public class EchoApplicationResource {

    //private String name;
    private ResourceDirectoryClientFactory factory;

    public EchoApplicationResource(ResourceDirectoryClientFactory factory) {
        this.factory = factory;
    }
    private static Map<String, AppManager> applicationMap = new HashMap<>();

    @GET
    @Timed
    @Produces(MediaType.APPLICATION_JSON)
    public EchoApplication get(@QueryParam("uuid") Optional<String> uuid) {
        if (uuid.isPresent())
            return new EchoApplication(uuid.get(), 0);
        else
            return new EchoApplication();
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public EchoApplication submit(@Valid String input) {
        String applicationId = UUID.randomUUID().toString();

        EchoApplication app = new EchoApplication(applicationId, 1);

        AppManager manager = new AppManager(applicationId, input);
        applicationMap.put(applicationId, manager);

        manager.run();
        return app;
    }

    @POST
    @Path("/stop/")
    @Produces(MediaType.APPLICATION_JSON)
    public EchoApplication stop(@QueryParam("uuid") String uuid) {
        AppManager manager = applicationMap.get(uuid);
        boolean flag = manager.stopDAG(uuid);
        if (flag) {
            applicationMap.remove(uuid);
            return new EchoApplication();
        }
        else
            return new EchoApplication();
    }

}
