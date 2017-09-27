package in.dream_lab.echo.rest_server;

import in.dream_lab.echo.rest_server.resources.EchoRestServerResource;
import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

/**
 * Created by pushkar on 8/2/17.
 */
public class EchoRestServerApplication extends Application<EchoRestServerConfiguration> {

    public static void main(String[] args) throws Exception {
        new EchoRestServerApplication().run(args);
    }

    @Override
    public String getName() {
        return "hello-world";
    }

    @Override
    public void initialize(Bootstrap<EchoRestServerConfiguration> bootstrap) {
        // nothing to do yet
    }

    @Override
    public void run(EchoRestServerConfiguration configuration,
                    Environment environment) {
        final EchoRestServerResource resource = new EchoRestServerResource(
                configuration.getTemplate(),
                configuration.getDeaultName()
        );
        environment.jersey().register(resource);
    }
}
