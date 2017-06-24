package in.dream_lab.echo.utils;

/**
 * Created by pushkar on 6/3/17.
 */
public class Connection {
    private String id;
    private String source;
    private String destination;
    private String sourceType;
    private String destinationType;
    private String sourceGroupId;
    private String destinationGroupId;
    private boolean deleted;

    public String getId() {return this.id;}
    public String getSource() {return this.source;}
    public String getDestination() {return this.destination;}
    public String getSourceType() {return this.sourceType;}
    public String getDestinationType() {return this.destinationType;}
    public String getSourceGroupId() {return this.sourceGroupId;}
    public String getDestinationGroupId() {return this.destinationGroupId;}
    public boolean isDeleted() {return this.deleted;}

    public void setId(String id) {this.id = id;}
    public void setSource(String source) {this.source = source;}
    public void setDestination(String destination) {this.destination = destination;}
    public void setSourceType(String sourceType) {this.sourceType = sourceType;}
    public void setDestinationType(String destinationType) {this.destinationType = destinationType;}
    public void setSourceGroupId(String sourceGroupId) {this.sourceGroupId = sourceGroupId;}
    public void setDestinationGroupId(String destinationGroupId) {this.destinationGroupId = destinationGroupId;}
    public void setDeleted(boolean deleted) {this.deleted = deleted;}
}
