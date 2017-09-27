package in.dream_lab.echo.utils;

import org.python.antlr.ast.Str;

/**
 * Created by pushkar on 1/9/17.
 */
public class ActualWiring {

    // type is either "PROCESSOR", "INPUT_PORT", "OUTPUT_PORT", "RPG"
    // id is not the nifi id

    public static String PROCESSOR = "PROCESSOR";
    public static String INPUT_PORT = "INPUT_PORT";
    public static String OUTPUT_PORT = "OUTPUT_PORT";
    public static String RPG = "RPG";
    public static String KAFKA_PORT = "KAFKA_PORT";

    public ActualWiring(String sourceType, int sourceId, String destinationType, int destinationId, String relationship) {
        setSourceType(sourceType);
        setDestinationType(destinationType);
        setSourceId(sourceId);
        setDestinationId(destinationId);
        setRelationship(relationship);
    }

    private String sourceType;
    private int sourceId;
    private String destinationType;
    private int destinationId;
    private String relationship;
    private String nifiId;

    public String getSourceType() {return this.sourceType;}
    public void setSourceType(String sourceType) {this.sourceType = sourceType;}

    public int getSourceId() {return this.sourceId;}
    public void setSourceId(int sourceId) {this.sourceId = sourceId;}

    public String getDestinationType() {return this.destinationType;}
    public void setDestinationType(String destinationType) {this.destinationType = destinationType;}

    public int getDestinationId() {return this.destinationId;}
    public void setDestinationId(int destinationId) {this.destinationId = destinationId;}

    public String getRelationship() {return this.relationship;}
    public void setRelationship(String relationship) {this.relationship = relationship;}

    public String getNifiId() {return this.nifiId;}
    public void setNifiId(String nifiId) {this.nifiId = nifiId;}

}
