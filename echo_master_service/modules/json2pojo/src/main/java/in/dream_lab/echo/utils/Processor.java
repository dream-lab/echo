
package in.dream_lab.echo.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
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
    "name",
    "class",
    "uuid",
    "nifi_id",
    "relationships",
    "config",
    "properties",
    "isInput",
    "sourceUuid"
})
public class Processor {

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("name")
    private String name;
    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("class")
    private String _class;
    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("uuid")
    private String uuid;
    @JsonProperty("nifi_id")
    private String nifiId;
    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("relationships")
    @JsonDeserialize(as = java.util.LinkedHashSet.class)
    private Set<Relationship> relationships = new LinkedHashSet<Relationship>();
    @JsonProperty("config")
    private List<Config> config = new ArrayList<Config>();
    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("properties")
    private List<Property> properties = new ArrayList<Property>();
    @JsonProperty("isInput")
    private Boolean isInput;
    @JsonProperty("sourceUuid")
    private String sourceUuid;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("name")
    public String getName() {
        return name;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("name")
    public void setName(String name) {
        this.name = name;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("class")
    public String getClass_() {
        return _class;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("class")
    public void setClass_(String _class) {
        this._class = _class;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("uuid")
    public String getUuid() {
        return uuid;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("uuid")
    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    @JsonProperty("nifi_id")
    public String getNifiId() {
        return nifiId;
    }

    @JsonProperty("nifi_id")
    public void setNifiId(String nifiId) {
        this.nifiId = nifiId;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("relationships")
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("relationships")
    public void setRelationships(Set<Relationship> relationships) {
        this.relationships = relationships;
    }

    @JsonProperty("config")
    public List<Config> getConfig() {
        return config;
    }

    @JsonProperty("config")
    public void setConfig(List<Config> config) {
        this.config = config;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("properties")
    public List<Property> getProperties() {
        return properties;
    }

    /**
     * 
     * (Required)
     * 
     */
    @JsonProperty("properties")
    public void setProperties(List<Property> properties) {
        this.properties = properties;
    }

    @JsonProperty("isInput")
    public Boolean getIsInput() {
        return isInput;
    }

    @JsonProperty("isInput")
    public void setIsInput(Boolean isInput) {
        this.isInput = isInput;
    }

    @JsonProperty("sourceUuid")
    public String getSourceUuid() {
        return sourceUuid;
    }

    @JsonProperty("sourceUuid")
    public void setSourceUuid(String sourceUuid) {
        this.sourceUuid = sourceUuid;
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
        return new HashCodeBuilder().append(uuid).toHashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if ((other instanceof Processor) == false) {
            return false;
        }
        Processor rhs = ((Processor) other);
        return new EqualsBuilder().append(name, rhs.name).append(_class, rhs._class).append(uuid, rhs.uuid).isEquals();
    }

}
