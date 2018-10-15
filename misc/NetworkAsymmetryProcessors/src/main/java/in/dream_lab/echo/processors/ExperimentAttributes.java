package in.dream_lab.echo.processors;

import org.apache.nifi.flowfile.attributes.FlowFileAttributeKey;

public enum ExperimentAttributes implements FlowFileAttributeKey{

    /**
     * Generates a unique ID that will be used to track a flowfiles passage through the system.
     */
    FLOWFILE_ID("flowfile_id");

    private final String key;

    private ExperimentAttributes(final String key) {
        this.key = key;
    }

    @Override
    public String key() {
        return key;
    }

}
