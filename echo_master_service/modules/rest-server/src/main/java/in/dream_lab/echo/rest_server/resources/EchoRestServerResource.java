package in.dream_lab.echo.rest_server.resources;

import com.codahale.metrics.annotation.Timed;
import in.dream_lab.echo.rest_server.api.Saying;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by pushkar on 8/2/17.
 */
@Path("/hello-world")
@Produces(MediaType.APPLICATION_JSON)
public class EchoRestServerResource {
    private final String template;
    private final String defaultName;
    private final AtomicLong counter;

    public EchoRestServerResource(String template, String defaultName) {
        this.template = template;
        this.defaultName = defaultName;
        this.counter = new AtomicLong();
    }

    @GET
    @Timed
    public Saying sayHello(@QueryParam("name") Optional<String> name) {
        final String value = String.format(template, name.orElse(defaultName));
        return new Saying(counter.incrementAndGet(), value);
    }
}
