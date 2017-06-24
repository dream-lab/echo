
package in.dream_lab.echo.utils;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
    "processors",
    "wiring",
    "inputstreams",
    "QOS"
})
public class DataflowInput {

    @JsonProperty("processors")
    @JsonDeserialize(as = java.util.LinkedHashSet.class)
    private Set<Processor> processors = new LinkedHashSet<Processor>();
    @JsonProperty("wiring")
    @JsonDeserialize(as = java.util.LinkedHashSet.class)
    private Set<Wiring> wiring = new LinkedHashSet<Wiring>();
    @JsonProperty("inputstreams")
    @JsonDeserialize(as = java.util.LinkedHashSet.class)
    private Set<String> inputstreams = new LinkedHashSet<String>();
    @JsonProperty("QOS")
    private Double qOS;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    @JsonProperty("processors")
    public Set<Processor> getProcessors() {
        return processors;
    }

    @JsonProperty("processors")
    public void setProcessors(Set<Processor> processors) {
        this.processors = processors;
    }

    @JsonProperty("wiring")
    public Set<Wiring> getWiring() {
        return wiring;
    }

    @JsonProperty("wiring")
    public void setWiring(Set<Wiring> wiring) {
        this.wiring = wiring;
    }

    @JsonProperty("inputstreams")
    public Set<String> getInputstreams() {
        return inputstreams;
    }

    @JsonProperty("inputstreams")
    public void setInputstreams(Set<String> inputstreams) {
        this.inputstreams = inputstreams;
    }

    @JsonProperty("QOS")
    public Double getQOS() {
        return qOS;
    }

    @JsonProperty("QOS")
    public void setQOS(Double qOS) {
        this.qOS = qOS;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }

    @JsonAnyGetter
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @JsonAnySetter
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(processors).append(wiring).append(inputstreams).append(qOS).append(additionalProperties).toHashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if ((other instanceof DataflowInput) == false) {
            return false;
        }
        DataflowInput rhs = ((DataflowInput) other);
        return new EqualsBuilder().append(processors, rhs.processors).append(wiring, rhs.wiring).append(inputstreams, rhs.inputstreams).append(qOS, rhs.qOS).append(additionalProperties, rhs.additionalProperties).isEquals();
    }

}
