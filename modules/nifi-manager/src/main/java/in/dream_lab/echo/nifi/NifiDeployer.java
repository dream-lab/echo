package in.dream_lab.echo.nifi;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import in.dream_lab.echo.utils.*;
import in.dream_lab.echo.master.AppDeployer;

import javax.sql.rowset.serial.SerialClob;

import java.util.*;

/**
 * Created by pushkar on 5/23/17.
 */
public class NifiDeployer implements AppDeployer {

    public NifiDeployer() {
    }

    public boolean startDag(Map<Processor, Device> placementMap) {
        startProcessors(placementMap);
        return true;
    }

    public boolean stopDag(Map<Processor, Device> placementMap) {
        stopProcessors(placementMap);
        return true;
    }

    public DataflowInput deployDag(Map<Processor, Device> placementMap, DataflowInput input) {
        for (Map.Entry<Processor, Device> entry : placementMap.entrySet()) {
            System.out.println(entry.getKey().getName() + " " + entry.getValue().getDeviceIP());
        }
        this.placeProcessors(placementMap);
        System.out.println("placement done");
        this.wireDag(placementMap, new ArrayList<>(input.getWiring()));
        this.startProcessors(placementMap);
        input.setProcessors(getProcessors(placementMap));
        return input;
    }

    public DataflowInput rebalanceDag(Map<Processor, Device> newPlacementMap, DataflowInput input, Map<String, Device> oldPlacementMap) {

        // create a datastructure of all connections/rpgs/input ports in all the respective devices.
        Map<Device, List<Connection>> connections = new HashMap<>();
        for (Map.Entry<String, Device> entry: oldPlacementMap.entrySet()) {
            if (connections.containsKey(entry.getValue()))
                continue;
            connections.put(entry.getValue(), entry.getValue().getClient().get_connections());
        }

        // find out the processors that moved
        List<Processor> movedProcessors = new ArrayList<>();
        Map<String, Processor> processorIdMap = new HashMap<>();
        for (Map.Entry<Processor, Device> entry : newPlacementMap.entrySet()) {
            Processor processor = entry.getKey();
            Device oldDevice = oldPlacementMap.get(processor.getUuid());
            Device newDevice = entry.getValue();
            processorIdMap.put(processor.getUuid(), processor);
            if (oldDevice.equals(newDevice))
                continue;

            movedProcessors.add(processor);
        }

        for (Map.Entry<Device, List<Connection>> entry : connections.entrySet()) {
            System.out.print(entry.getKey().getDeviceIP());
            for (Connection connection : entry.getValue())
                System.out.println(connection.getId());
            System.out.println("------");
        }

        // This loops through all the connections and breaks them if necessary
        System.out.println("About to remove the wirings");
        for (Wiring wire : input.getWiring()) {
            String sourceNifiId = processorIdMap.get(wire.getSourceProcessor()).getNifiId();
            String destinationNifiId = processorIdMap.get(wire.getDestinationProcessor()).getNifiId();
            Device sourceDevice = oldPlacementMap.get(processorIdMap.get(wire.getSourceProcessor()).getUuid());
            Device destinationDevice = oldPlacementMap.get(processorIdMap.get(wire.getDestinationProcessor()).getUuid());
            NifiClient sourceClient = sourceDevice.getClient();
            NifiClient destinationClient = destinationDevice.getClient();
            sourceDevice.getClient().stop_processor(sourceNifiId);
            destinationDevice.getClient().stop_processor(destinationNifiId);
            if (!(movedProcessors.contains(processorIdMap.get(wire.getSourceProcessor())) ||
                    movedProcessors.contains(processorIdMap.get(wire.getDestinationProcessor()))))
                continue;

            System.out.println("This one needs to be removed");
            if (sourceDevice.equals(destinationDevice)) {
                // remove the connection
                for (Connection connection : connections.get(sourceDevice)) {
                    if (connection.getSource() == sourceNifiId && connection.getDestination() == destinationNifiId)  {
                        if (connection.isDeleted())
                            break;
                        sourceClient.delete_connection(connection.getId());
                        connection.setDeleted(true);
                        break;
                    }
                }
            } else {
                // remove rpg,
                for (Connection connection : connections.get(sourceDevice)) {
                    System.out.println("This one is being deleted?");
                    if (connection.isDeleted())
                        continue;
                    boolean flag = false;
                    if (connection.getSource().equals(sourceNifiId) && connection.getDestinationType().equals("REMOTE_INPUT_PORT")) {
                        System.out.println("please tell me its going in here");
                        for (Connection destConnection : connections.get(destinationDevice)) {
                            if (destConnection.isDeleted())
                                continue;
                            if (destConnection.getDestination().equals(destinationNifiId) && destConnection.getSource().equals(connection.getDestination())) {
                                String inputPort = connection.getDestination();
                                String rpg = connection.getDestinationGroupId();
                                String sourceConnection = connection.getId();
                                String destinationConnection = destConnection.getId();
                                connection.setDeleted(true);


                                try {
                                    while (!sourceClient.disable_rpg_transmission(rpg)) Thread.sleep(100);
                                    System.out.println("RPG disabled!");
                                    while (!sourceClient.delete_connection(sourceConnection)) Thread.sleep(100);
                                    System.out.println("connection deleted");
                                    while (!sourceClient.delete_remote_process_group(rpg)) Thread.sleep(100);
                                    System.out.println("rpg deleted");

                                    while (!destinationClient.disable_input_port(inputPort)) Thread.sleep(100);
                                    System.out.println("input port disabled");
                                    while (!destinationClient.delete_connection(destinationConnection))
                                        Thread.sleep(100);
                                    System.out.println("connection deleted");
                                    while (!destinationClient.delete_input_port(inputPort)) Thread.sleep(100);
                                    System.out.println("input port deleted");
                                    destConnection.setDeleted(true);

                                    flag = true;
                                    break;
                                } catch(InterruptedException e) {
                                    e.printStackTrace();
                                }

                            }
                        }
                    }
                    if (flag) break;
                }
            }
        }

        //move the processors from old device to the new device
        for (Processor processor : movedProcessors) {
            Device newDevice = newPlacementMap.get(processor);
            NifiClient oldClient = oldPlacementMap.get(processor.getUuid()).getClient();
            NifiClient newClient = newDevice.getClient();

            Map<String, Object> properties = new HashMap<>();
            for (Property p : processor.getProperties())
                for(Map.Entry<String, Object> P : p.getAdditionalProperties().entrySet())
                    properties.put(P.getKey(), P.getValue());

            Map<String, Object> configs = new HashMap<>();
            for (Config c : processor.getConfig())
                for(Map.Entry<String, Object> C : c.getAdditionalProperties().entrySet())
                    configs.put(C.getKey(), C.getValue());


            ObjectMapper mapper = new ObjectMapper();
            try {
                String propertiesJson = mapper.writeValueAsString(properties);
                String relationshipsJson = mapper.writeValueAsString(processor.getRelationships());
                String configsJson = mapper.writeValueAsString(configs);
                oldClient.delete_processor(processor.getNifiId());
                String processorId = newClient.create_processor(processor.getClass_(), processor.getName());
                newClient.set_processor_properties_and_relationships(processorId, propertiesJson, relationshipsJson, configsJson);
                newPlacementMap.remove(processor);
                processor.setNifiId(processorId);
                newPlacementMap.put(processor, newDevice);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        }

        //reconnect the stuff
        for (Wiring wire : input.getWiring()) {
            if (movedProcessors.contains(processorIdMap.get(wire.getSourceProcessor()))
                    || movedProcessors.contains(processorIdMap.get(wire.getDestinationProcessor()))) {
                String source = wire.getSourceProcessor();
                String destination = wire.getDestinationProcessor();
                System.out.println(source + " " + destination);
                Processor sourceProcessor = null;
                Processor destinationProcessor = null;
                Device sourceDevice = null;
                Device destinationDevice = null;
                for (Map.Entry<Processor, Device> entry : newPlacementMap.entrySet()) {
                    System.out.println("searching");
                    if (sourceProcessor == null && entry.getKey().getUuid().equals(source)) {
                        System.out.println("its the source");
                        sourceProcessor = entry.getKey();
                        sourceDevice = entry.getValue();
                    } else if (destinationProcessor == null && entry.getKey().getUuid().equals(destination)) {
                        System.out.println("its the dest");
                        destinationProcessor = entry.getKey();
                        destinationDevice = entry.getValue();
                    }
                }
                System.out.println(sourceProcessor.getNifiId());
                System.out.println(destinationProcessor.getNifiId());
                System.out.println(newPlacementMap.size());
                //System.out.println(placementMap.get(sourceProcessor).getClient().get_url());

                if (sourceDevice.equals(destinationDevice)) {
                    connectProcessors(destinationDevice, sourceProcessor, destinationProcessor, wire.getSelectedRelationships());
                } else {
                    connectRpgs(sourceDevice, sourceProcessor, destinationDevice, destinationProcessor, wire.getSelectedRelationships());
                }
            }
        }
        startProcessors(newPlacementMap);

        return input;
    }

    private void stopAndDisconnectProcessors(String sourceProcessor, Device source, String destinationProcessor, Device destination) {

    }

    private Set<Processor> getProcessors(Map<Processor, Device> placementMap) {
        Set<Processor> set = new HashSet<>();
        for (Map.Entry<Processor, Device> entry : placementMap.entrySet())
            set.add(entry.getKey());
        return set;
    }

    /*public boolean deployDag(Map<Processor, Device> placementMap, List<Wiring> wiring) {
        // TODO
        this.placeProcessors(placementMap);
        this.wireDag(placementMap, wiring);
        this.startProcessors(placementMap);
        return true;
    }*/

    private void placeProcessors(Map<Processor, Device> placementMap) {
        // TODO, is it better to use the Processor object here or just the processor uuid?

        for (Map.Entry<Processor, Device> entry : placementMap.entrySet()) {
            // Put entry.getKey into entry.getValue
            Processor processor = entry.getKey();
            NifiClient client = entry.getValue().getClient();
            System.out.println(client.get_url());


            String processorID = client.create_processor(processor.getClass_(), processor.getName());
            processor.setNifiId(processorID);
            Map<String, Object> properties = new HashMap<>();
            for (Property p : processor.getProperties()){
                for(Map.Entry<String, Object> P : p.getAdditionalProperties().entrySet()){
                    properties.put(P.getKey(), P.getValue());
                }
            }
            Map<String, Object> configs = new HashMap<>();
            for (Config c : processor.getConfig())
                for(Map.Entry<String, Object> C : c.getAdditionalProperties().entrySet())
                    configs.put(C.getKey(), C.getValue());
            ObjectMapper mapper = new ObjectMapper();
            try {
                //String propertiesJson = mapper.writeValueAsString(properties);
                //String relationshipsJson = mapper.writeValueAsString(processor.getRelationships());
                String propertiesJson = mapper.writeValueAsString(properties);
                String relationshipsJson = mapper.writeValueAsString(processor.getRelationships());
                String configsJson = mapper.writeValueAsString(configs);
                System.out.println("------" + configsJson + "------");
                client.set_processor_properties_and_relationships(processorID, propertiesJson, relationshipsJson, configsJson);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        }
    }

    private void connectProcessors(Device device, Processor source, Processor destination, Set<String> relationships) {
        NifiClient client = device.getClient();
        String relationship = new String();
        Iterator<String> rIterator = relationships.iterator();
        // TODO this handles only single relationship. Make it handle multiple
        if (rIterator.hasNext())
            relationship = rIterator.next();
        client.create_connection(source.getNifiId(), destination.getNifiId(), relationship);
        return;
    }

    private void connectRpgs(Device sourceDevice, Processor source, Device destinationDevice, Processor destination, Set<String> relationships) {
        NifiClient sourceClient = sourceDevice.getClient();
        NifiClient destinationClient = destinationDevice.getClient();

        // add input port on destination side
        String port_id = null;
        do {
            try {
                port_id = destinationClient.create_new_input_port(source.getUuid() + " " + destination.getUuid());
                System.out.println("what is this" + port_id);
                Thread.sleep(1000);
            } catch(InterruptedException e) {
                e.printStackTrace();
            }
        } while(port_id == null);
        do {
            System.out.println(port_id);
        }
        while (!destinationClient.create_connection(port_id, destination.getNifiId(), null));
        destinationClient.enable_input_port(port_id);
        // add rpg on source side
        String rpg_id = sourceClient.create_remote_process_group(destinationClient.get_url());
        sourceClient.connect_remote_input_port(rpg_id, port_id, source.getNifiId(), relationships.iterator().next());
        sourceClient.enable_rpg_transmission(rpg_id);

        return;
    }

    private void wireDag(Map<Processor, Device> placementMap, List<Wiring> wirings) {
        System.out.println("here");
        System.out.println(placementMap.size()+ " " + wirings.size());
        for (Wiring wire : wirings){
            String source = wire.getSourceProcessor();
            String destination = wire.getDestinationProcessor();
            System.out.println(source + " " + destination);
            Processor sourceProcessor = null;
            Processor destinationProcessor = null;
            Device sourceDevice = null;
            Device destinationDevice = null;
            for (Map.Entry<Processor, Device> entry : placementMap.entrySet()) {
                System.out.println("searching");
                if (sourceProcessor == null && entry.getKey().getUuid().equals(source)) {
                    System.out.println("its the source");
                    sourceProcessor = entry.getKey();
                    sourceDevice = entry.getValue();
                } else if (destinationProcessor == null && entry.getKey().getUuid().equals(destination)) {
                    System.out.println("its the dest");
                    destinationProcessor = entry.getKey();
                    destinationDevice = entry.getValue();
                }
            }
            System.out.println(sourceProcessor.getNifiId());
            System.out.println(destinationProcessor.getNifiId());
            System.out.println(placementMap.size());
            //System.out.println(placementMap.get(sourceProcessor).getClient().get_url());

            if (sourceDevice.equals(destinationDevice)) {
                connectProcessors(destinationDevice, sourceProcessor, destinationProcessor, wire.getSelectedRelationships());
            } else {
                connectRpgs(sourceDevice, sourceProcessor, destinationDevice, destinationProcessor, wire.getSelectedRelationships());
            }
        }
    }

    public void startProcessors(Map<Processor, Device> placementMapping) {
        for(Map.Entry<Processor, Device> entry : placementMapping.entrySet()) {
            NifiClient client = entry.getValue().getClient();
            String processorId = entry.getKey().getNifiId();
            client.start_processor(processorId);
        }
    }

    private void stopProcessors(Map<Processor, Device> placementMap) {
        for(Map.Entry<Processor, Device> entry : placementMap.entrySet()) {
            NifiClient client = entry.getValue().getClient();
            String processorId = entry.getKey().getNifiId();
            client.stop_processor(processorId);
        }
    }

    public static void main(String args[]) {

    }

}
