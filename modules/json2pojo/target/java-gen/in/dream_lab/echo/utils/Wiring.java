
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
    "sourceProcessor",
    "destinationProcessor",
    "selectedRelationships"
})
public class Wiring {

    @JsonProperty("sourceProcessor")
    private String sourceProcessor;
    @JsonProperty("destinationProcessor")
    private String destinationProcessor;
    @JsonProperty("selectedRelationships")
    @JsonDeserialize(as = java.util.LinkedHashSet.class)
    private Set<String> selectedRelationships = new LinkedHashSet<String>();
    @JsonIgnore
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    @JsonProperty("sourceProcessor")
    public String getSourceProcessor() {
        return sourceProcessor;
    }

    @JsonProperty("sourceProcessor")
    public void setSourceProcessor(String sourceProcessor) {
        this.sourceProcessor = sourceProcessor;
    }

    @JsonProperty("destinationProcessor")
    public String getDestinationProcessor() {
        return destinationProcessor;
    }

    @JsonProperty("destinationProcessor")
    public void setDestinationProcessor(String destinationProcessor) {
        this.destinationProcessor = destinationProcessor;
    }

    @JsonProperty("selectedRelationships")
    public Set<String> getSelectedRelationships() {
        return selectedRelationships;
    }

    @JsonProperty("selectedRelationships")
    public void setSelectedRelationships(Set<String> selectedRelationships) {
        this.selectedRelationships = selectedRelationships;
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
        return new HashCodeBuilder().append(sourceProcessor).append(destinationProcessor).append(selectedRelationships).append(additionalProperties).toHashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if ((other instanceof Wiring) == false) {
            return false;
        }
        Wiring rhs = ((Wiring) other);
        return new EqualsBuilder().append(sourceProcessor, rhs.sourceProcessor).append(destinationProcessor, rhs.destinationProcessor).append(selectedRelationships, rhs.selectedRelationships).append(additionalProperties, rhs.additionalProperties).isEquals();
    }

}
