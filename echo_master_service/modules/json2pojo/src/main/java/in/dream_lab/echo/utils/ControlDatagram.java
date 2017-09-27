package in.dream_lab.echo.utils;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by pushkar on 31/8/17.
 */
public class ControlDatagram {

    @JsonProperty
    private String resourceId;

    @JsonProperty
    private String sessionId;

    @JsonProperty
    private Map<Integer, ControlMethod> methodSet = new HashMap<>();

    @JsonProperty
    private String ackTopic;

    public String getResourceId() { return this.resourceId; }
    public void setResourceId(String resourceId) { this.resourceId = resourceId; }

    public String getSessionId() { return this.sessionId; }
    public void setSessionId(String sessionId) { this.sessionId = sessionId; }

    public String getAckTopic() {return this.ackTopic;}
    public void setAckTopic(String ackTopic) {this.ackTopic = ackTopic;}

    public Map<Integer, ControlMethod> getMethodSet() {return this.methodSet;}
    public void setMethodSet(Map<Integer, ControlMethod> methodSet) {this.methodSet = methodSet;}

    public void addMethod(ControlMethod method) {this.methodSet.put(method.getSequenceId(), method);}
}
