package in.dream_lab.echo.utils;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by pushkar on 2/9/17.
 */
public class ResponseDatagram {
    @JsonProperty
    private String resourceId;

    @JsonProperty
    private String sessionId;

    @JsonProperty
    private Map<String, ControlResponse> responseSet = new HashMap<>();

    public String getResourceId() { return this.resourceId; }
    public void setResourceId(String resourceId) { this.resourceId = resourceId; }

    public String getSessionId() { return this.sessionId; }
    public void setSessionId(String sessionId) { this.sessionId = sessionId; }

    public Map<String, ControlResponse> getResponseSet() {return this.responseSet;}
    public void setResponseSet(Map<String, ControlResponse> methodSet) {this.responseSet = methodSet;}
}
