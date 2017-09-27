package in.dream_lab.echo.utils;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

/**
 * Created by pushkar on 31/8/17.
 */
public class ControlMethod {

    @JsonProperty
    private String methodName;

    @JsonProperty
    private int sequenceId;

    @JsonProperty
    private Map<String, String> params;

    public String getMethodName() { return this.methodName; }
    public void setMethodName(String methodName) {this.methodName = methodName; }

    public int getSequenceId() {return this.sequenceId; }
    public void setSequenceId(int sequenceId) { this.sequenceId = sequenceId; }

    public Map<String, String> getParams() { return this.params; }
    public void setParams(Map<String, String> params) {this.params = params; }
}
