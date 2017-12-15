package in.dream_lab.echo.utils;

import org.apache.commons.lang.builder.EqualsBuilder;

/**
 * Created by pushkar on 9/15/17.
 */
public class NifiKafkaPort {

    public NifiKafkaPort(boolean consumer, int id, String topic) {
        this.consumer = consumer;
        this.id = id;
        this.topic = topic;
    }

    private boolean consumer;
    private int id;
    private String nifiId;
    private String topic;

    public boolean isConsumer() { return this.consumer; }
    public void setConsumer(boolean input) { this.consumer = input;}

    public int getId() { return this.id; }
    public void setId(int id) { this.id = id; }

    public String getNifiId() { return this.nifiId; }
    public void setNifiId(String nifiId) { this.nifiId = nifiId; }

    public String getTopic() { return this.topic; }
    public void setTopic(String topic) { this.topic = topic; }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if ((other instanceof NifiKafkaPort) == false) {
            return false;
        }
        NifiKafkaPort rhs = ((NifiKafkaPort) other);
        return new EqualsBuilder().append(id, rhs.id).isEquals();
    }

}
