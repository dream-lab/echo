package in.dream_lab.echo.utils;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by pushkar on 2/9/17.
 */
public class ControlResponse {

    @JsonProperty
    String id;

    @JsonProperty
    String type;

    public String getId() {return this.id;}
    public void setId(String id) {this.id = id;}

    public String getType() {return this.type;}
    public void setType(String type) {this.type = type;}
}
