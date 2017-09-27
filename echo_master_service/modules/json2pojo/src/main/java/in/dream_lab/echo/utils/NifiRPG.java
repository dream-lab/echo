package in.dream_lab.echo.utils;

/**
 * Created by pushkar on 1/9/17.
 */
public class NifiRPG {

    public NifiRPG(int id, int processorId, String remoteUrl, String relationship) {
        this.id = id;
        this.nifiId = null;
        this.processorId = processorId;
        this.remoteUrl = remoteUrl;
        this.relationship = relationship;
    }

    public NifiRPG(int id, int processorId, String remoteUrl) {
        this(id, processorId, remoteUrl, null);
    }

    private int id;
    private String nifiId;
    private int processorId;
    private String relationship;
    private String remoteUrl;

    public int getId() {return this.id;}
    public void setId(int id) {this.id = id;}

    public String getNifiId() {return this.nifiId;}
    public void setNifiId(String nifiId) {this.nifiId = nifiId;}

    public String getRelationship() { return this.relationship; }
    public void setRelationship(String relationship) {this.relationship = relationship;}

    public String getRemoteUrl() {return this.remoteUrl;}
    public void setRemoteUrl(String remoteUrl) {this.remoteUrl = remoteUrl;}

    public int getProcessorId() {return this.processorId;}
    public void setProcessorId(int processorId) {this.processorId = processorId;}
}
