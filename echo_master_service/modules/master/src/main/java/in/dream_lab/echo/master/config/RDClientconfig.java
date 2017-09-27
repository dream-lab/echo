package in.dream_lab.echo.master.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.hibernate.validator.constraints.NotEmpty;

/**
 * Created by pushkar on 8/3/17.
 */
public class RDClientconfig {

    @NotEmpty
    @JsonProperty
    private String rdAddress;

    @NotEmpty
    @JsonProperty
    private int portNumber;

    public String getRdAddress() {
        return this.rdAddress;
    }

    public int getPortNumber() {
        return this.portNumber;
    }

}
