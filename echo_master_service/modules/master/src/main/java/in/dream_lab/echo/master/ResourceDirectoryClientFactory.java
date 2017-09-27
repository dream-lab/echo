package in.dream_lab.echo.master;

/**
 * Created by pushkar on 8/10/17.
 */
public class ResourceDirectoryClientFactory {

    private String address;
    private int port;

    public ResourceDirectoryClientFactory(String address, int port) {
        this.address = address;
        this.port = port;
    }

    public ResourceDirectory getClient() {
        return new ResourceDirectory(address, port);
    }

}
