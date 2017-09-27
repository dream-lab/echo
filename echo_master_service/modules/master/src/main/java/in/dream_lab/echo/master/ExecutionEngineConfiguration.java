package in.dream_lab.echo.master;

import com.fasterxml.jackson.annotation.JsonProperty;
import in.dream_lab.echo.master.config.RDClientconfig;
import io.dropwizard.Configuration;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.NotNull;

/**
 * Created by pushkar on 8/3/17.
 */
public class ExecutionEngineConfiguration extends Configuration {

    @NotNull
    @JsonProperty
    private RDClientconfig rdClientConfig;

    @NotEmpty
    @JsonProperty
    private String defaultName = "Stranger";

    public RDClientconfig getRdClientconfig() {
        return this.rdClientConfig;
    }

    public String getDefaultName() {
        return this.defaultName;
    }

}
