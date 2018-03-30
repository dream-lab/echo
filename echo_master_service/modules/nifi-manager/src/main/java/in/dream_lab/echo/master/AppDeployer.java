package in.dream_lab.echo.master;

import in.dream_lab.echo.utils.DataflowInput;
import in.dream_lab.echo.utils.Device;
import in.dream_lab.echo.utils.Processor;
import in.dream_lab.echo.utils.Wiring;

import java.util.List;
import java.util.Map;

/**
 * Created by pushkar on 5/16/17.
 */
public interface AppDeployer {

    // This class has a function that will need to be invoked by the master
    // whenever the application has to be deployed or rebalanced across the
    // various devices in the edge. The question is, how to design this class?
    // Each manager should make a single synchronous call to the deployer to

    //public boolean deployDag(Map<Processor, Device> placementMap, List<Wiring> wiring);
    public boolean stopDag() throws Exception;
    public DataflowInput deployDag(Map<Processor, Device> placementMap, DataflowInput input) throws Exception;
    public boolean rebalanceDag(Map<Processor, Device> placementMap, DataflowInput input) throws Exception;

}
