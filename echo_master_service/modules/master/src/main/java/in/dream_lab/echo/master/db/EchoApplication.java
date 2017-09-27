package in.dream_lab.echo.master.db;

import com.fasterxml.jackson.annotation.JsonProperty;
import in.dream_lab.echo.utils.DataflowInput;
import in.dream_lab.echo.utils.Device;
import in.dream_lab.echo.utils.Processor;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Created by pushkar on 8/10/17.
 */
public class EchoApplication {

    public EchoApplication() {
        this.uuid = UUID.randomUUID().toString();
        this.version = 0;
    }

    public EchoApplication(String uuid, int version) {
        this.uuid = uuid;
        this.version = version;
    }

    @JsonProperty
    private DataflowInput dataflowInput = new DataflowInput();

    @JsonProperty
    private String uuid;

    @JsonProperty
    private String name = new String();

    @JsonProperty
    private int version;

    @JsonProperty
    private String status = new String(); // TODO have an enum for Status

    @JsonProperty
    private String owner = new String(); // TODO a class for Owner

    @JsonProperty
    private Map<Processor, Device> deviceMapping = new HashMap<>();

    // TODO add reference to the AppManager
    // Needs to have the

    public DataflowInput getDataflowInput() {
        return this.dataflowInput;
    }
    public void setDataflowInput(DataflowInput input) {
        this.dataflowInput = input;
    }

    public String getUuid() {
        return this.uuid;
    }

    public String getName() {
        return this.name;
    }
    public void setName(String name) {
        this.name = name;
    }

    public int getVersion() {
        return this.version;
    }

    public String getStatus() {
        return this.status;
    }
    public void setStatus(String status) {
        // TODO change the status here. Or change the function name
        this.status = status;
    }

    public String getOwner() {
        return this.owner;
    }
    public void setOwner(String owner) {
        this.owner = owner;
    }

    public Map<Processor, Device> getDeviceMapping() {
        return this.deviceMapping;
    }
    public void setDeviceMapping(Map<Processor, Device> deviceMapping) {
        this.deviceMapping = deviceMapping;
    }

    // TODO the methods of App Manager go here. (Maybe have a class inside here?)
}
