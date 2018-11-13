package in.dream_lab.echo.nifi;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import in.dream_lab.echo.master.NetworkVisibilityMatrix;
import in.dream_lab.echo.utils.*;
import in.dream_lab.echo.master.AppDeployer;
import jnr.ffi.annotations.In;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.dom4j.bean.BeanAttributeList;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import javax.sql.rowset.serial.SerialClob;
import javax.xml.ws.Response;

import java.util.*;
import java.util.function.Predicate;

/**
 * Created by pushkar on 5/23/17.
 */
public class NifiDeployer implements AppDeployer {

    Random random = new Random();
    String sessionId = UUID.randomUUID().toString();
    MqttClient mqttClient;

    private Map<Processor, Device> processorMapping;
    private NetworkVisibilityMatrix matrix;
    private Map<Device, List<Processor>> iProcessorMap;
    private Map<Device, List<NifiCommPort>> iPortMap;
    private Map<Device, List<NifiRPG>> iRPGMap;
    private Map<Device, List<NifiKafkaPort>> iKafkaMap;
    private Map<Device, List<ActualWiring>> iWiringMap;
    private Set<ActualWiring> iGlobalWiring;

    public NifiDeployer(MqttClient mqttClient) {
        this.mqttClient = mqttClient;
    }

    public NifiDeployer(MqttClient mqttClient, NetworkVisibilityMatrix matrix) {
        this.mqttClient = mqttClient;
        this.matrix = matrix;
    }

    private Processor getProcessorFromUUID(int UUID) {
        Processor toReturn = null;
        for (Map.Entry<Processor, Device> entry : this.processorMapping.entrySet()) {
            if (entry.getKey().getUuid().equals(Integer.toString(UUID))) {
                toReturn = entry.getKey();
                break;
            }
        }
        return toReturn;
    }

    private NifiCommPort getNifiCommPortFromUUID(int UUID, Device device) {
        NifiCommPort toReturn = null;
        for (NifiCommPort port : iPortMap.get(device)) {
            if (port.getId() == UUID) {
                toReturn = port;
                break;
            }
        }
        return toReturn;
    }

    private NifiRPG getNifiRPGFromUUID(int UUID, Device device) {
        NifiRPG toReturn = null;
        for (NifiRPG port : iRPGMap.get(device)) {
            if (port.getId() == UUID) {
                toReturn = port;
                break;
            }
        }
        return toReturn;
    }

    private NifiKafkaPort getNifiKafkaPortFromUUID(int UUID, Device device) {
        NifiKafkaPort toReturn = null;
        for (NifiKafkaPort port : iKafkaMap.get(device)) {
            if (port.getId() == UUID) {
                toReturn = port;
                break;
            }
        }
        return toReturn;
    }

    // **********************************************************************************
    // ************************** REBALANCE Beginning ***********************************
    // **********************************************************************************
    public boolean rebalanceDag(Map<Processor, Device> newPlacementMap, DataflowInput input) throws Exception {
        // ID diff
        Set<Processor> processorsToMove = findMappingDiff(newPlacementMap);
        System.out.println("Size of diffset is " + processorsToMove.size());
        for (Processor p : processorsToMove) {
            System.out.println("to move " + p.getUuid());
        }

        // getAdjacentAssets
        // ****************************THis is where you are!!!!************************************
        Pair<Pair<Set<Processor>, Set<NifiCommPort>>, Pair<Set<NifiRPG>, Set<NifiKafkaPort>>> assets = getAdjacentAssets(processorsToMove);

        Set<Processor> processorsToPause = assets.getLeft().getLeft();
        Set<NifiCommPort> portsToRemove = assets.getLeft().getRight();
        Set<NifiRPG> rpgsToRemove = assets.getRight().getLeft();
        Set<NifiKafkaPort> kafkaPortsToRemove = assets.getRight().getRight();

        processorsToPause.addAll(processorsToMove);

        System.out.println("*");
        for (Processor p : processorsToPause) {
            System.out.println(p.getUuid());
        }
        System.out.println("*");
        for (NifiCommPort p : portsToRemove) {
            System.out.println(p.getNifiId());
        }
        System.out.println("*");
        for (NifiRPG p : rpgsToRemove) {
            System.out.println(p.getNifiId());
        }
        System.out.println("*");

        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setCleanSession(true);
        try {
            mqttClient.connect();

            stopAssets(processorsToMove, processorsToPause, portsToRemove, rpgsToRemove, kafkaPortsToRemove);
            System.out.println("assets stopped");
            removeAssets(processorsToMove, processorsToPause, portsToRemove, rpgsToRemove, kafkaPortsToRemove);
            System.out.println("assets removed");
            reconnect(processorsToMove, processorsToPause, newPlacementMap, input.getWiring(), portsToRemove,
                    rpgsToRemove, kafkaPortsToRemove);

            mqttClient.disconnect();
        } catch (MqttException e) {
            e.printStackTrace();
        }
        this.processorMapping = newPlacementMap;

        return true;
    }

    private int getLastKafkaId(Map<Device, List<NifiKafkaPort>> map) {
        int id = 0;
        for (Map.Entry<Device, List<NifiKafkaPort>> entry : map.entrySet()) {
            for (NifiKafkaPort port : entry.getValue()) {
                if (port.getId() > id)
                    id = port.getId();
            }
        }
        return id;
    }

    private int getLastPortId(Map<Device, List<NifiCommPort>> map) {
        int id = 0;
        for (Map.Entry<Device, List<NifiCommPort>> entry : map.entrySet()) {
            for (NifiCommPort port : entry.getValue()) {
                if (port.getId() > id)
                    id = port.getId();
            }
        }
        return id;
    }

    private int getLastRpgId(Map<Device, List<NifiRPG>> map) {
        int id = 0;
        for (Map.Entry<Device, List<NifiRPG>> entry : map.entrySet()) {
            for (NifiRPG rpg : entry.getValue()) {
                if (rpg.getId() > id)
                    id = rpg.getId();
            }
        }
        return id;
    }

    private Map<Device, List<Processor>> populateNewProcessors(Set<Processor> processors, Map<Processor, Device> newMap) {
        Map<Device, List<Processor>> toReturn = new HashMap<>();
        for (Processor processor : processors) {
            Device device = newMap.get(processor);
            if (toReturn.containsKey(device)) {
                List<Processor> toAdd = new ArrayList<>();
                if (toReturn.get(device) != null) {
                    toAdd = toReturn.get(device);
                }
                toAdd.add(processor);
                toReturn.put(device, toAdd);
            } else {
                List<Processor> toAdd = new ArrayList<>();
                toAdd.add(processor);
                toReturn.put(device, toAdd);
            }
        }

        return toReturn;
    }

    private void reconnect(Set<Processor> processorsToMove, Set<Processor> processorsToPause,
                           Map<Processor, Device> newPlacementMap, Set<Wiring> wiring, Set<NifiCommPort> portsToRemove,
                           Set<NifiRPG> rpgsToRemove, Set<NifiKafkaPort> kafkaPortsToRemove) throws MqttException, Exception {

        Map<Device, List<Processor>> newProcessors = populateNewProcessors(processorsToMove, newPlacementMap);
        Map<Device, List<NifiCommPort>> newPorts = new HashMap<>();
        Map<Device, List<NifiRPG>> newRPGs = new HashMap<>();
        Map<Device, List<NifiKafkaPort>> newKafkaPorts = new HashMap<>();
        Map<Device, List<ActualWiring>> newWirings = new HashMap<>();
        Set<ActualWiring> newGlobalWirings = new HashSet<>();

        int rpgCounter = getLastRpgId(iRPGMap) + 1;
        int portCounter = getLastPortId(iPortMap) + 1;
        int kafkaCounter = getLastKafkaId(iKafkaMap) + 1;

        for (Wiring wire : wiring) {
            String relationship = new String();
            if (!wire.getSelectedRelationships().isEmpty())
                relationship = wire.getSelectedRelationships().iterator().next();

            String source = wire.getSourceProcessor(), destination = wire.getDestinationProcessor();
            Processor sourceProcessor = null, destinationProcessor = null;
            Device sourceDevice = null, destinationDevice = null;

            for (Map.Entry<Processor, Device> entry : newPlacementMap.entrySet()) {
                if (sourceProcessor == null && entry.getKey().getUuid().equals(source)) {
                    sourceProcessor = entry.getKey();
                    sourceDevice = entry.getValue();
                } else if (destinationProcessor == null && entry.getKey().getUuid().equals(destination)) {
                    destinationProcessor = entry.getKey();
                    destinationDevice = entry.getValue();
                }
            }

            if (!processorsToMove.contains(sourceProcessor) && !processorsToMove.contains(destinationProcessor))
                continue;

            int sourceId = Integer.parseInt(source), destinationId = Integer.parseInt(destination);

            if (sourceDevice.equals(destinationDevice)) {
                ActualWiring newWiring = new ActualWiring(
                        ActualWiring.PROCESSOR, sourceId, ActualWiring.PROCESSOR, destinationId, relationship);
                newWirings = addNewWiring(sourceDevice, newWiring, newWirings);
            } else {
                String direction = matrix.getDirection(
                        Integer.parseInt(sourceDevice.getDeviceUUID()),
                        Integer.parseInt(destinationDevice.getDeviceUUID()));

                if (!direction.equals(NetworkVisibilityMatrix.NOT_VISIBLE)) {
                    NifiRPG rpg;
                    NifiCommPort port;

                    if (direction.equals(NetworkVisibilityMatrix.PUSH_DIRECTION)) {
                        rpg = new NifiRPG(rpgCounter++, sourceId, destinationDevice.getUrl(), relationship);
                        port = new NifiCommPort(true, portCounter++);
                        ActualWiring newWiring = new ActualWiring(
                                ActualWiring.INPUT_PORT, port.getId(), ActualWiring.PROCESSOR,
                                destinationId, null);

                        newRPGs = addNewRPG(sourceDevice, rpg, newRPGs);
                        newPorts = addNewPort(destinationDevice, port, newPorts);
                        newWirings = addNewWiring(destinationDevice, newWiring, newWirings);
                        newGlobalWirings.add(new ActualWiring(
                                ActualWiring.RPG, rpg.getId(), ActualWiring.INPUT_PORT, port.getId(), null));
                    } else {
                        rpg = new NifiRPG(rpgCounter++, destinationId, sourceDevice.getUrl(), null);
                        port = new NifiCommPort(false, portCounter++);
                        ActualWiring newWiring = new ActualWiring(
                                ActualWiring.PROCESSOR, sourceId, ActualWiring.OUTPUT_PORT, port.getId(), relationship);

                        newPorts = addNewPort(sourceDevice, port, newPorts);
                        newRPGs = addNewRPG(destinationDevice, rpg, newRPGs);
                        newWirings = addNewWiring(sourceDevice, newWiring, newWirings);
                        newGlobalWirings.add(new ActualWiring(
                                ActualWiring.OUTPUT_PORT, port.getId(), ActualWiring.RPG, rpg.getId(), null));
                    }
                } else {
                    String topic = UUID.randomUUID().toString();
                    NifiKafkaPort publisher = new NifiKafkaPort(false, kafkaCounter++, topic);
                    NifiKafkaPort consumer = new NifiKafkaPort(true, kafkaCounter++, topic);

                    newWirings = addNewWiring(sourceDevice, new ActualWiring(
                            ActualWiring.PROCESSOR, sourceId, ActualWiring.KAFKA_PORT, publisher.getId(), relationship),
                            newWirings);
                    newWirings = addNewWiring(destinationDevice, new ActualWiring(
                            ActualWiring.KAFKA_PORT, consumer.getId(), ActualWiring.PROCESSOR, destinationId, "success"),
                            newWirings);
                    newGlobalWirings.add(new ActualWiring(
                            ActualWiring.KAFKA_PORT, publisher.getId(), ActualWiring.KAFKA_PORT, consumer.getId(), null));

                    newKafkaPorts = addNewKafkaPort(sourceDevice, publisher, newKafkaPorts);
                    newKafkaPorts = addNewKafkaPort(destinationDevice, consumer, newKafkaPorts);
                }
            }
        }

        /********************Now to push the stuff into the devices
         *
         */

        Set<Device> devices = new HashSet<>();
        for (Device device : newPlacementMap.values()) {
            boolean flag = true;
            for (Device device1 : devices) {
                if (device.equals(device1)) {
                    flag = false;
                    break;
                }
            }
            if (flag) devices.add(device);
        }
        Map<Integer, Device> resourceIdMap = getResourceIdMap(devices);
        System.out.println(devices.size());
        for (Device device : processorMapping.values()) {
            boolean flag = true;
            for (Device device1 : devices) {
                if (device.equals(device1)) {
                    flag = false;
                    break;
                }
            }
            if (flag) devices.add(device);
        }
        System.out.println(devices.size());

        for (Device device : devices)
            System.out.println(device.getDeviceIP());
        // might not traverse those processors that do not have new processors but have ports and stuff.
        // what do I loop over?!
        int datagramCount = 0;
        for (Device device : devices) {

            String resourceId = device.getDeviceUUID();
            String mqttTopic = resourceId;
            ControlDatagram datagram = new ControlDatagram();
            datagram.setResourceId(resourceId);
            datagram.setSessionId(sessionId);
            datagram.setAckTopic(sessionId);

            List<Processor> processors = new ArrayList<>();
            List<NifiCommPort> ports = new ArrayList();
            List<NifiRPG> rpgs = new ArrayList<>();
            List<NifiKafkaPort> kafkaPorts = new ArrayList<>();

            if (newProcessors.get(device) != null)
                processors = newProcessors.get(device);
            if (newPorts.get(device) != null)
                ports = newPorts.get(device);
            if (newRPGs.get(device) != null)
                rpgs = newRPGs.get(device);
            if (newKafkaPorts.get(device) != null)
                kafkaPorts = newKafkaPorts.get(device);

            newProcessors.put(device, processors);
            newPorts.put(device, ports);
            newRPGs.put(device, rpgs);
            newKafkaPorts.put(device, kafkaPorts);

            int sequence = 1;

            for (Processor processor : processors) {

                ControlMethod method = new ControlMethod();
                Map<String, String> params = new HashMap<>();

                method.setMethodName("create_processor");
                method.setSequenceId(sequence++);
                params.put("class", processor.getClass_());
                params.put("name", processor.getName());

                String propertiesJson = "";
                String relationshipsJson = "";
                String configsJson = "";
                Map<String, Object> properties = new HashMap<>();
                for (Property p : processor.getProperties())
                    for (Map.Entry<String, Object> P : p.getAdditionalProperties().entrySet())
                        properties.put(P.getKey(), P.getValue());

                Map<String, Object> configs = new HashMap<>();
                for (Config c : processor.getConfig())
                    for (Map.Entry<String, Object> C : c.getAdditionalProperties().entrySet())
                        configs.put(C.getKey(), C.getValue());

                ObjectMapper mapper = new ObjectMapper();
                try {
                    propertiesJson = mapper.writeValueAsString(properties);
                    relationshipsJson = mapper.writeValueAsString(processor.getRelationships());
                    configsJson = mapper.writeValueAsString(configs);
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
                params.put("properties", propertiesJson);
                params.put("relationships", relationshipsJson);
                params.put("configs", configsJson);

                method.setParams(params);
                datagram.addMethod(method);
                // create processor
            }
            for (NifiCommPort port : ports) {
                // create port
                ControlMethod method = new ControlMethod();
                String methodName;
                if (port.isInput())
                    methodName = "create_input_port";
                else
                    methodName = "create_output_port";
                Map<String, String> params = new HashMap<>();
                params.put("name", sessionId + " " + Integer.toString(port.getId()));
                method.setMethodName(methodName);
                method.setSequenceId(sequence++);
                method.setParams(params);
                datagram.addMethod(method);
            }
            for (NifiRPG rpg : rpgs) {
                // create rpg
                ControlMethod method = new ControlMethod();
                method.setMethodName("create_rpg");
                method.setSequenceId(sequence++);
                Map<String, String> params = new HashMap<>();
                params.put("url", rpg.getRemoteUrl());
                method.setParams(params);
                datagram.addMethod(method);
            }
            for (NifiKafkaPort kafkaPort : kafkaPorts) {
                ControlMethod method = new ControlMethod();
                if (kafkaPort.isConsumer())
                    method.setMethodName("create_kafka_consumer");
                else
                    method.setMethodName("create_kafka_producer");
                method.setSequenceId(sequence++);
                Map<String, String> params = new HashMap<>();
                params.put("topic", kafkaPort.getTopic());
                method.setParams(params);
                datagram.addMethod(method);
            }

            if (sequence == 1)
                continue;

            String payloadJson = "";
            try {
                ObjectMapper objectMapper = new ObjectMapper();
                payloadJson = objectMapper.writeValueAsString(datagram);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            System.out.println("payload to " + resourceId);
            System.out.println(payloadJson);
            // publish to Mqtt
            MqttMessage message = new MqttMessage(payloadJson.getBytes());
            message.setQos(2);
            mqttClient.publish(mqttTopic, message);
            datagramCount++;
        }
        Map<Integer, ResponseDatagram> responses =
                ControlResponseReceiver.receiveResponse(datagramCount, sessionId, mqttClient.getServerURI());
        for (Map.Entry<Integer, ResponseDatagram> entry : responses.entrySet()) {
            Integer resourceId = entry.getKey();
            Device device = resourceIdMap.get(resourceId);
            ResponseDatagram response = entry.getValue();
            Map<String, ControlResponse> responseMap = response.getResponseSet();
            int i = 1;
            for (Processor processor : newProcessors.get(device)) {
                String id = Integer.toString(i);
                String nifiId = responseMap.get(id).getId();
                processor.setNifiId(nifiId);
                i++;
            }
            for (NifiCommPort port : newPorts.get(device)) {
                String id = Integer.toString(i);
                String nifiId = responseMap.get(id).getId();
                port.setNifiId(nifiId);
                i++;
            }
            for (NifiRPG rpg : newRPGs.get(device)) {
                String id = Integer.toString(i);
                String nifiId = responseMap.get(id).getId();
                rpg.setNifiId(nifiId);
                i++;
            }
            for (NifiKafkaPort kafkaPort : newKafkaPorts.get(device)) {
                String id = Integer.toString(i);
                String nifiId = responseMap.get(id).getId();
                kafkaPort.setNifiId(nifiId);
                i++;
            }
        }

        for (Map.Entry<Device, List<Processor>> entry : iProcessorMap.entrySet()) {
            Device device = entry.getKey();
            entry.getValue().removeIf(new Predicate<Processor>() {
                @Override
                public boolean test(Processor processor) {
                    return processorsToMove.contains(processor);
                }
            });
            iPortMap.get(device).removeIf(new Predicate<NifiCommPort>() {
                @Override
                public boolean test(NifiCommPort nifiCommPort) {
                    return portsToRemove.contains(nifiCommPort);
                }
            });
            iRPGMap.get(device).removeIf(new Predicate<NifiRPG>() {
                @Override
                public boolean test(NifiRPG nifiRPG) {
                    return rpgsToRemove.contains(nifiRPG);
                }
            });
            iKafkaMap.get(device).removeIf(new Predicate<NifiKafkaPort>() {
                @Override
                public boolean test(NifiKafkaPort nifiKafkaPort) {
                    return kafkaPortsToRemove.contains(nifiKafkaPort);
                }
            });
            entry.getValue().addAll(newProcessors.get(device));
            iPortMap.get(device).addAll(newPorts.get(device));
            iRPGMap.get(device).addAll(newRPGs.get(device));
            iKafkaMap.get(device).addAll(newKafkaPorts.get(device));
        }

        for (Map.Entry<Device, List<Processor>> entry : newProcessors.entrySet()) {
            if (iProcessorMap.get(entry.getKey()) == null)
                iProcessorMap.put(entry.getKey(), entry.getValue());
        }
        for (Map.Entry<Device, List<NifiCommPort>> entry : newPorts.entrySet()) {
            if (iPortMap.get(entry.getKey()) == null)
                iPortMap.put(entry.getKey(), entry.getValue());
        }
        for (Map.Entry<Device, List<NifiRPG>> entry : newRPGs.entrySet()) {
            if (iRPGMap.get(entry.getKey()) == null)
                iRPGMap.put(entry.getKey(), entry.getValue());
        }
        for (Map.Entry<Device, List<NifiKafkaPort>> entry : newKafkaPorts.entrySet()) {
            if (iKafkaMap.get(entry.getKey()) == null)
                iKafkaMap.put(entry.getKey(), entry.getValue());
        }
        for (Map.Entry<Device, List<ActualWiring>> entry : newWirings.entrySet()) {
             /*if (iWiringMap.get(entry.getKey()) == null)
                iWiringMap.put(entry.getKey(), entry.getValue());*/
        	List<ActualWiring> updatedWirings = iWiringMap.get(entry.getKey());
        	if(updatedWirings == null)
        		updatedWirings = new ArrayList<>();
        	updatedWirings.addAll(entry.getValue());
        	iWiringMap.put(entry.getKey(), updatedWirings);
        }

        for (Map.Entry<Device, List<Processor>> entry : newProcessors.entrySet()) {

        }

        /**
         * If we go from a configuration containing more devices to a configuration containing
         * fewer devices, then we can remove the devices which are not in use anymore. Those devices
         * won't contain any processors,rpgs or any ports.
         */
        Iterator<Device> deviceIter = iProcessorMap.keySet().iterator();
        while(deviceIter.hasNext()) {
        	Device device = deviceIter.next();
        	if((iProcessorMap.get(device) == null || iProcessorMap.get(device).isEmpty()) &&
        			(iRPGMap.get(device) == null || iRPGMap.get(device).isEmpty()) &&
        			(iPortMap.get(device) == null || iPortMap.get(device).isEmpty()) &&
        			(iKafkaMap.get(device) == null || iKafkaMap.get(device).isEmpty()) &&
        			(iWiringMap.get(device) == null || iWiringMap.get(device).isEmpty())) {
        		System.out.println("This device has no elements attached to it, so safe to remove it");
        		System.out.println("Removing device with UUID : " + device.getDeviceUUID());
        		/*
        		 * Since we are iterating over iProcessorMap, do not remove explicitly remove
        		 * from the collection, remove using the iterator
        		 */
        		deviceIter.remove();
        		iRPGMap.remove(device);
        		iPortMap.remove(device);
        		iKafkaMap.remove(device);
        		iWiringMap.remove(device);
        		System.out.println("Done with removing unused elements");
        	}
        }



        /****Now, Connect these assets
         *
         */

        datagramCount = 0;
        for (Map.Entry<Device, List<ActualWiring>> entry : newWirings.entrySet()) {
            Device device = entry.getKey();
            String mqttTopic = device.getDeviceUUID();
            List<ActualWiring> wirings = entry.getValue();
            ControlDatagram datagram = new ControlDatagram();
            datagram.setResourceId(device.getDeviceUUID());
            datagram.setAckTopic(sessionId);
            datagram.setSessionId(sessionId);

            int sequence = 1;
            for (ActualWiring wire : wirings) {
                ControlMethod method = new ControlMethod();
                method.setMethodName("create_connection");
                method.setSequenceId(sequence++);

                String sourceNifiId = getNifiId(
                        wire.getSourceId(), wire.getSourceType(), iProcessorMap.get(device), iPortMap.get(device),
                        iRPGMap.get(device), iKafkaMap.get(device));
                String destinationNifiId = getNifiId(
                        wire.getDestinationId(), wire.getDestinationType(), iProcessorMap.get(device), iPortMap.get(device),
                        iRPGMap.get(device), iKafkaMap.get(device));
                Map<String, String> params = new HashMap<>();
                params.put("from_id", sourceNifiId);
                params.put("to_id", destinationNifiId);
                params.put("relationship_name", wire.getRelationship());

                method.setParams(params);
                datagram.addMethod(method);
            }
            if (datagram.getMethodSet().size() == 0)
                continue;
            datagramCount++;
            String payloadJson = "";

            try {
                ObjectMapper objectMapper = new ObjectMapper();
                payloadJson = objectMapper.writeValueAsString(datagram);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }

            MqttMessage message = new MqttMessage(payloadJson.getBytes());
            message.setQos(2);
            mqttClient.publish(mqttTopic, message);
        }
        responses =
                ControlResponseReceiver.receiveResponse(datagramCount, sessionId, mqttClient.getServerURI());
        for (Map.Entry<Integer, ResponseDatagram> entry : responses.entrySet()) {
            Integer resourceId = entry.getKey();
            Device device = resourceIdMap.get(resourceId);
            ResponseDatagram response = entry.getValue();
            Map<String, ControlResponse> responseMap = response.getResponseSet();
            int i = 1;
            for (ActualWiring wire : newWirings.get(device)) {
                String id = Integer.toString(i);
                String nifiId = responseMap.get(id).getId();
                wire.setNifiId(nifiId);
                i++;
            }
        }

       /** 
         * START - INPUT PORT ISSUE
         * Enable the input ports . The handling of input port is different from output ports
         * in the platform service. An output port on creation is enabled by default but an 
         * input port is not enabled by default and should be enabled.
         * Currently added this piece only for input port. If this works, then similar thing
         * must be done for kafka port as well
         */
        datagramCount = 0;
        for (Map.Entry<Device, List<NifiCommPort>> entry : newPorts.entrySet()) {
        	String mqttTopic = entry.getKey().getDeviceUUID();
            ControlDatagram datagram = new ControlDatagram();
            datagram.setAckTopic(sessionId);
            datagram.setSessionId(sessionId);
            datagram.setResourceId(entry.getKey().getDeviceUUID());
            int sequence = 1;
            for (NifiCommPort port : entry.getValue()) {
                if (!port.isInput())
                    continue;
                ControlMethod method = new ControlMethod();
                method.setMethodName("enable_input_port");
                method.setSequenceId(sequence++);
                Map<String, String> params = new HashMap<>();
                params.put("port_id", port.getNifiId());
                method.setParams(params);
                datagram.addMethod(method);
            } 
            if (sequence == 1)
                continue;
            String payloadJson = "";
            try {
                ObjectMapper objectMapper = new ObjectMapper();
                payloadJson = objectMapper.writeValueAsString(datagram);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            System.out.println(payloadJson);
            // publish to Mqtt
            MqttMessage message = new MqttMessage(payloadJson.getBytes());
            message.setQos(2);
            mqttClient.publish(mqttTopic, message);
            datagramCount++;
        }
        responses =
                ControlResponseReceiver.receiveResponse(datagramCount, sessionId, mqttClient.getServerURI());   

        /**
         * END - INPUT PORT ISSUE
         */

        /*
         * The new kafka ports are not enabled, this is to handle them
         */
		datagramCount = 0;
		for (Map.Entry<Device, List<NifiKafkaPort>> entry : newKafkaPorts.entrySet()) {
			String mqttTopic = entry.getKey().getDeviceUUID();
			ControlDatagram datagram = new ControlDatagram();
			datagram.setAckTopic(sessionId);
			datagram.setSessionId(sessionId);
			datagram.setResourceId(entry.getKey().getDeviceUUID());
			int sequence = 1;
			for (NifiKafkaPort port : entry.getValue()) {
				ControlMethod method = new ControlMethod();
				method.setMethodName("enable_kafka_port");
				method.setSequenceId(sequence++);
				Map<String, String> params = new HashMap<>();
				params.put("port_id", port.getNifiId());
				method.setParams(params);
				datagram.addMethod(method);
			}
			if (sequence == 1)
				continue;
			String payloadJson = "";
			try {
				ObjectMapper objectMapper = new ObjectMapper();
				payloadJson = objectMapper.writeValueAsString(datagram);
			} catch (JsonProcessingException e) {
				e.printStackTrace();
			}
			System.out.println(payloadJson);
			// publish to Mqtt
			MqttMessage message = new MqttMessage(payloadJson.getBytes());
			message.setQos(2);
			mqttClient.publish(mqttTopic, message);
			datagramCount++;
		}
		responses = ControlResponseReceiver.receiveResponse(datagramCount, sessionId, mqttClient.getServerURI());


        datagramCount = 0;
        for (ActualWiring wire : newGlobalWirings) {
            if (wire.getSourceType() == ActualWiring.KAFKA_PORT)
                continue;
            String sourceType = wire.getSourceType();
            ControlDatagram datagram = new ControlDatagram();
            datagram.setSessionId(sessionId);
            datagram.setAckTopic(sessionId);
            ControlMethod method = new ControlMethod();
            method.setSequenceId(1);
            int rpgId;
            int portId;
            if (sourceType.equals(ActualWiring.RPG)) {
                // input port case
                rpgId = wire.getSourceId();
                portId = wire.getDestinationId();
                method.setMethodName("connect_remote_input_port");
            } else {
                // output port case
                portId = wire.getSourceId();
                rpgId = wire.getDestinationId();
                method.setMethodName("connect_remote_output_port");
            }
            Map<String, String> params = new HashMap<>();
            // get device for rpg
            // get NifiId for rpg
            Pair<Device, NifiRPG> pair = getDeviceAndRpg(rpgId, newRPGs);
            Device device = pair.getLeft();
            NifiRPG rpg = pair.getRight();
            params.put("rpg", rpg.getNifiId());
            params.put("port_id", getPortId(portId, newPorts));
            params.put("proc_id", getProcId(rpg.getProcessorId(), iProcessorMap));
            params.put("relationship", rpg.getRelationship());
            // get portId for rpg
            method.setParams(params);
            datagram.setResourceId(device.getDeviceUUID());
            datagram.addMethod(method);

            datagramCount++;
            String payloadJson = "";
            try {
                ObjectMapper objectMapper = new ObjectMapper();
                payloadJson = objectMapper.writeValueAsString(datagram);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }

            MqttMessage message = new MqttMessage(payloadJson.getBytes());
            message.setQos(2);
            String mqttTopic = device.getDeviceUUID();
            mqttClient.publish(mqttTopic, message);
        }
        responses =
                ControlResponseReceiver.receiveResponse(datagramCount, sessionId, mqttClient.getServerURI());


       /**
         * iGlobalWiring is not updated which will create issues in the next rebalance. 
         * Check if this is the case and if so, raise issue in github.
         */
        iGlobalWiring.addAll(newGlobalWirings);


        /**
         * Critical Bug: Assume we move from a configuration EFC(2:1:5) to EFC(2:0:6)
         * wherein no processor now on Fog, in such a scenario (first of all remove those
         * devices which have no part to play, however this is done in a separate issue) 
         * still sending message to such device will never get you a reply and master
         * will be unresponsive.
         */
        datagramCount = 0;
        for (Map.Entry<Device, List<Processor>> entry : iProcessorMap.entrySet()) {
            String mqttTopic = entry.getKey().getDeviceUUID();
            ControlDatagram datagram = new ControlDatagram();
            datagram.setAckTopic(sessionId);
            datagram.setSessionId(sessionId);
            datagram.setResourceId(entry.getKey().getDeviceUUID());
            int sequence = 1;
            for (Processor processor : entry.getValue()) {
                ControlMethod method = new ControlMethod();
                method.setMethodName("start_processor");
                method.setSequenceId(sequence++);
                Map<String, String> params = new HashMap<>();
                params.put("proc_id", processor.getNifiId());
                method.setParams(params);
                datagram.addMethod(method);
            }

            /**
             * A possible fix for the above mentioned critical issue
             */
            if(sequence == 1)
            	continue;

            String payloadJson = "";
            try {
                ObjectMapper objectMapper = new ObjectMapper();
                payloadJson = objectMapper.writeValueAsString(datagram);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            System.out.println(payloadJson);
            // publish to Mqtt
            MqttMessage message = new MqttMessage(payloadJson.getBytes());
            message.setQos(2);
            mqttClient.publish(mqttTopic, message);
            datagramCount++;
        }
        responses =
                ControlResponseReceiver.receiveResponse(datagramCount, sessionId, mqttClient.getServerURI());

    }

    private void removeAssets(Set<Processor> processorsToMove, Set<Processor> processorsToPause,
                            Set<NifiCommPort> portsToRemove, Set<NifiRPG> rpgsToRemove,
                            Set<NifiKafkaPort> kafkaPortsToRemove) throws Exception {
        Iterator<ActualWiring> wirings = iGlobalWiring.iterator();
        while (wirings.hasNext()) {
            ActualWiring wire = wirings.next();
            if (wire.getSourceType().equals(ActualWiring.RPG) &&
                    rpgsToRemove.contains(getDeviceAndRpg(wire.getSourceId(), iRPGMap).getRight()))
                wirings.remove();
            else if (wire.getDestinationType().equals(ActualWiring.RPG) &&
                    rpgsToRemove.contains(getDeviceAndRpg(wire.getDestinationId(), iRPGMap).getRight()))
                wirings.remove();
        }
        int datagramCount = 0;
        for (Map.Entry<Device, List<Processor>> entry : this.iProcessorMap.entrySet()) {
            Device device = entry.getKey();

            String mqttTopic = device.getDeviceUUID();
            ControlDatagram datagram = new ControlDatagram();
            datagram.setAckTopic(sessionId);
            datagram.setSessionId(sessionId);
            datagram.setResourceId(device.getDeviceUUID());
            int sequence = 1;
            Iterator<Processor> iter = entry.getValue().iterator();
            while (iter.hasNext()) {
                Processor processor = iter.next();
                if (processorsToMove.contains(processor)) {
                    ControlMethod method = new ControlMethod();
                    method.setMethodName("remove_processor");
                    method.setSequenceId(sequence++);
                    Map<String, String> params = new HashMap<>();
                    params.put("proc_id", processor.getNifiId());
                    method.setParams(params);
                    datagram.addMethod(method);
                    iter.remove();
                }
            }

            Iterator<NifiRPG> iterrpg = iRPGMap.get(device).iterator();
            while (iterrpg.hasNext()) {
                NifiRPG rpg = iterrpg.next();
                if (rpgsToRemove.contains(rpg)) {
                    ControlMethod method = new ControlMethod();
                    method.setMethodName("remove_rpg");
                    method.setSequenceId(sequence++);
                    Map<String, String> params = new HashMap<>();
                    params.put("rpg_id", rpg.getNifiId());
                    method.setParams(params);
                    datagram.addMethod(method);
                    iterrpg.remove();
                }
            }
            Iterator<NifiCommPort> iterport = iPortMap.get(device).iterator();
            while (iterport.hasNext()) {
                NifiCommPort port = iterport.next();
                if (portsToRemove.contains(port)) {
                    ControlMethod method = new ControlMethod();
                    if (port.isInput())
                        method.setMethodName("remove_input_port");
                    else
                        method.setMethodName("remove_output_port");
                    method.setSequenceId(sequence++);
                    Map<String, String> params = new HashMap<>();
                    params.put("port_id", port.getNifiId());
                    method.setParams(params);
                    datagram.addMethod(method);
                    iterport.remove();
                }
            }

            Iterator<NifiKafkaPort> iterkafka = iKafkaMap.get(device).iterator();
            while(iterkafka.hasNext()) {
                NifiKafkaPort kafkaPort = iterkafka.next();
                if (kafkaPortsToRemove.contains(kafkaPort)) {
                    ControlMethod method = new ControlMethod();
                    method.setMethodName("remove_kafka_port");
                    method.setSequenceId(sequence++);
                    Map<String, String> params = new HashMap<>();
                    params.put("port_id", kafkaPort.getNifiId());
                    method.setParams(params);
                    datagram.addMethod(method);
                    iterkafka.remove();
                }
            }

            if (sequence == 1)
                continue;

            String payloadJson = "";
            try {
                ObjectMapper objectMapper = new ObjectMapper();
                payloadJson = objectMapper.writeValueAsString(datagram);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            MqttMessage message = new MqttMessage(payloadJson.getBytes());
            message.setQos(2);
            try {
                mqttClient.publish(mqttTopic, message);
            } catch (MqttException e) {
                e.printStackTrace();
            }

            datagramCount++;
        }
        Map<Integer, ResponseDatagram> responses =
                ControlResponseReceiver.receiveResponse(datagramCount, sessionId, mqttClient.getServerURI());

    }

    private void stopAssets(Set<Processor> processorsToMove, Set<Processor> processorsToPause,
                              Set<NifiCommPort> portsToRemove, Set<NifiRPG> rpgsToRemove,
                              Set<NifiKafkaPort> kafkaPortsToRemove) throws Exception {
        int datagramCount = 0;
        for (Map.Entry<Device, List<Processor>> entry : this.iProcessorMap.entrySet()) {
            Device device = entry.getKey();

            String mqttTopic = device.getDeviceUUID();
            ControlDatagram datagram = new ControlDatagram();
            datagram.setAckTopic(sessionId);
            datagram.setSessionId(sessionId);
            datagram.setResourceId(device.getDeviceUUID());
            int sequence = 1;
            for (Processor processor : entry.getValue()) {
                if (processorsToPause.contains(processor)) {
                    ControlMethod method = new ControlMethod();
                    method.setMethodName("stop_processor");
                    method.setSequenceId(sequence++);
                    Map<String, String> params = new HashMap<>();
                    params.put("proc_id", processor.getNifiId());
                    method.setParams(params);
                    datagram.addMethod(method);
                }
            }
            for (NifiRPG rpg : iRPGMap.get(entry.getKey())) {
                if (rpgsToRemove.contains(rpg)) {
                    ControlMethod method = new ControlMethod();
                    method.setMethodName("disable_rpg");
                    method.setSequenceId(sequence++);
                    Map<String, String> params = new HashMap<>();
                    params.put("rpg_id", rpg.getNifiId());
                    method.setParams(params);
                    datagram.addMethod(method);
                }
            }
            for (NifiCommPort port : iPortMap.get(entry.getKey())) {
                if (portsToRemove.contains(port)) {
                    ControlMethod method = new ControlMethod();
                    if (port.isInput())
                        method.setMethodName("disable_input_port");
                    else
                        method.setMethodName("disable_output_port");
                    method.setSequenceId(sequence++);
                    Map<String, String> params = new HashMap<>();
                    params.put("port_id", port.getNifiId());
                    method.setParams(params);
                    datagram.addMethod(method);
                }
            }
            for (NifiKafkaPort kafkaPort : iKafkaMap.get(entry.getKey())) {
                if (kafkaPortsToRemove.contains(kafkaPort)) {
                    ControlMethod method = new ControlMethod();
                    method.setMethodName("disable_kafka_port");
                    method.setSequenceId(sequence++);
                    Map<String, String> params = new HashMap<>();
                    params.put("port_id", kafkaPort.getNifiId());
                    method.setParams(params);
                    datagram.addMethod(method);

                }
            }

            Iterator<ActualWiring> iter = null;
            if(this.iWiringMap.containsKey(device)) {
                iter = this.iWiringMap.get(device).iterator();
            }
            while (iter != null && iter.hasNext()) {
                ActualWiring wiring = iter.next();
                if ( (wiring.getSourceType().equals(ActualWiring.PROCESSOR) &&
                    processorsToMove.contains(getProcessorFromUUID(wiring.getSourceId()))) ||
                    (wiring.getDestinationType().equals(ActualWiring.PROCESSOR) &&
                        processorsToMove.contains(getProcessorFromUUID(wiring.getDestinationId()))) ||
                    (wiring.getSourceType().equals(ActualWiring.INPUT_PORT) &&
                        portsToRemove.contains(getDeviceAndPort(wiring.getSourceId(), iPortMap).getRight())) ||
                    (wiring.getDestinationType().equals(ActualWiring.OUTPUT_PORT) &&
                        portsToRemove.contains((getDeviceAndPort(wiring.getDestinationId(), iPortMap).getRight()))) ||
                    (wiring.getSourceType().equals(ActualWiring.KAFKA_PORT) &&
                        kafkaPortsToRemove.contains(getDeviceAndKafkaPort(wiring.getSourceId(), iKafkaMap).getRight())) ||
                    (wiring.getDestinationType().equals(ActualWiring.KAFKA_PORT) &&
                        kafkaPortsToRemove.contains(getDeviceAndKafkaPort(wiring.getDestinationId(), iKafkaMap).getRight()))) {

                        ControlMethod method = new ControlMethod();
                        method.setMethodName("purge_connection");
                        method.setSequenceId(sequence++);
                        Map<String, String> params = new HashMap<>();
                        params.put("connection_id", wiring.getNifiId());
                        method.setParams(params);
                        datagram.addMethod(method);
                        iter.remove();
                }
            }

            if (sequence == 1)
                continue;

            String payloadJson = "";
            try {
                ObjectMapper objectMapper = new ObjectMapper();
                payloadJson = objectMapper.writeValueAsString(datagram);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            MqttMessage message = new MqttMessage(payloadJson.getBytes());
            message.setQos(2);
            try {
                mqttClient.publish(mqttTopic, message);
            } catch (MqttException e) {
                e.printStackTrace();
            }

            datagramCount++;
        }
        Map<Integer, ResponseDatagram> responses =
                ControlResponseReceiver.receiveResponse(datagramCount, sessionId, mqttClient.getServerURI());
    }

    private Pair<Pair<Set<Processor>, Set<NifiCommPort>>, Pair<Set<NifiRPG>, Set<NifiKafkaPort>>>
    getAdjacentAssets(Set<Processor> processorsToMove) {
        Set<Processor> processorsToPause = new HashSet<>();
        Set<NifiCommPort> portsToRemove = new HashSet<>();
        Set<NifiRPG> rpgsToRemove = new HashSet<>();
        Set<NifiKafkaPort> kafkaPortsToRemove = new HashSet<>();

        // find adjacent assets in the same device
        // number 1, 2, 3 and 4 here
        for (Processor processor : processorsToMove) {
            for (ActualWiring wiring : iWiringMap.get(this.processorMapping.get(processor))){
                if (wiring.getSourceType().equals(ActualWiring.PROCESSOR)
                        && wiring.getSourceId() == Integer.parseInt(processor.getUuid())) {
                    if (wiring.getDestinationType().equals(ActualWiring.PROCESSOR))
                        processorsToPause.add(getProcessorFromUUID(wiring.getDestinationId()));
                    else if (wiring.getDestinationType().equals(ActualWiring.OUTPUT_PORT)) {
                        NifiCommPort port = getNifiCommPortFromUUID(wiring.getDestinationId(), this.processorMapping.get(processor));
                        portsToRemove.add(port);
                    }
                    else if (wiring.getDestinationType().equals(ActualWiring.KAFKA_PORT)){
                        NifiKafkaPort port = getNifiKafkaPortFromUUID(wiring.getDestinationId(), this.processorMapping.get(processor));
                        kafkaPortsToRemove.add(port);
                    }
                } else if (wiring.getDestinationType().equals(ActualWiring.PROCESSOR)
                        && wiring.getDestinationId() == Integer.parseInt(processor.getUuid())) {
                    if (wiring.getSourceType().equals(ActualWiring.PROCESSOR))
                        processorsToPause.add(getProcessorFromUUID(wiring.getSourceId()));
                    else if (wiring.getSourceType().equals(ActualWiring.INPUT_PORT)) {
                        NifiCommPort port = getNifiCommPortFromUUID(wiring.getSourceId(), this.processorMapping.get(processor));
                        portsToRemove.add(port);
                    }
                    else if (wiring.getSourceType().equals(ActualWiring.KAFKA_PORT)) {
                        NifiKafkaPort port = getNifiKafkaPortFromUUID(wiring.getSourceId(), this.processorMapping.get(processor));
                        kafkaPortsToRemove.add(port);
                    }
                }
            }
            for (ActualWiring wiring : iGlobalWiring) {
                if (wiring.getSourceType().equals(ActualWiring.RPG)) {
                    NifiRPG rpg = getDeviceAndRpg(wiring.getSourceId(), iRPGMap).getRight();
                    if (rpg.getProcessorId() == Integer.parseInt(processor.getUuid())){
                        rpgsToRemove.add(rpg);
                        portsToRemove.add(getDeviceAndPort(wiring.getDestinationId(), iPortMap).getRight());
                    }
                } else if (wiring.getDestinationType().equals(ActualWiring.RPG)) {
                    NifiRPG rpg = getDeviceAndRpg(wiring.getDestinationId(), iRPGMap).getRight();
                    if (rpg.getProcessorId() == Integer.parseInt(processor.getUuid())){
                        rpgsToRemove.add(rpg);
                        portsToRemove.add(getDeviceAndPort(wiring.getSourceId(), iPortMap).getRight());
                    }
                }
            }
        }
        // 5 here
        for (NifiCommPort port : portsToRemove) {
            for (ActualWiring wiring : iGlobalWiring) {
                if (wiring.getSourceType().equals(ActualWiring.OUTPUT_PORT)
                        && wiring.getSourceId() == port.getId()) {
                    rpgsToRemove.add(getDeviceAndRpg(wiring.getDestinationId(), iRPGMap).getRight());
                    break;
                } else if (wiring.getDestinationType().equals(ActualWiring.INPUT_PORT)
                        && wiring.getDestinationId() == port.getId()) {
                    rpgsToRemove.add(getDeviceAndRpg(wiring.getSourceId(), iRPGMap).getRight());
                    break;
                }
            }
            // get the device for this port and pass through the wirings
            /*Device device = getDeviceAndPort(port.getId(), iPortMap).getLeft();
            for (ActualWiring wiring : iWiringMap.get(device)) {
                if (wiring.getSourceType().equals(ActualWiring.INPUT_PORT)
                        && wiring.getSourceId() == port.getId()) {
                    // TODO take care of this assumption.
                    // Assuming ports are directly connected to only the processors
                    processorsToPause.add(getProcessorFromUUID(wiring.getDestinationId()));
                } else if (wiring.getDestinationType().equals(ActualWiring.OUTPUT_PORT)
                        && wiring.getDestinationId() == port.getId()) {
                    processorsToPause.add(getProcessorFromUUID(wiring.getSourceId()));
                }
            }*/
        }
        // 6 and 9 here
        for (NifiRPG rpg : rpgsToRemove) {
            for (ActualWiring wiring : iGlobalWiring) {
                if (wiring.getSourceType().equals(ActualWiring.RPG)
                        && wiring.getSourceId() == rpg.getId()) {
                    portsToRemove.add(getDeviceAndPort(wiring.getDestinationId(), iPortMap).getRight());
                    processorsToPause.add(getProcessorFromUUID(rpg.getProcessorId()));
                    break;
                } else if (wiring.getDestinationType().equals(ActualWiring.RPG)
                        && wiring.getDestinationId() == rpg.getId()) {
                    portsToRemove.add(getDeviceAndPort(wiring.getSourceId(), iPortMap).getRight());
                    processorsToPause.add(getProcessorFromUUID(rpg.getProcessorId()));
                    break;
                }
            }
        }

        // 7 here
        Set<NifiKafkaPort> tmpSet = new HashSet<>();
        for (NifiKafkaPort port : kafkaPortsToRemove) {
            for (ActualWiring wiring : iGlobalWiring) {
                if (wiring.getSourceType().equals(ActualWiring.KAFKA_PORT)
                        && wiring.getSourceId() == port.getId()) {
                    tmpSet.add(getDeviceAndKafkaPort(wiring.getDestinationId(), iKafkaMap).getRight());
                    break;
                } else if (wiring.getDestinationType().equals(ActualWiring.KAFKA_PORT)
                        && wiring.getDestinationId() == port.getId()) {
                    tmpSet.add(getDeviceAndKafkaPort(wiring.getSourceId(), iKafkaMap).getRight());
                    break;
                }
            }
        }
        kafkaPortsToRemove.addAll(tmpSet);

        // 8 here
        for (NifiCommPort port : portsToRemove) {
            Device device = getDeviceAndPort(port.getId(), iPortMap).getLeft();
            for (ActualWiring wiring : iWiringMap.get(device)) {
                if (wiring.getSourceType().equals(ActualWiring.INPUT_PORT)
                        && wiring.getSourceId() == port.getId()) {
                    // TODO take care of this assumption.
                    // Assuming ports are directly connected to only the processors
                    processorsToPause.add(getProcessorFromUUID(wiring.getDestinationId()));
                } else if (wiring.getDestinationType().equals(ActualWiring.OUTPUT_PORT)
                        && wiring.getDestinationId() == port.getId()) {
                    processorsToPause.add(getProcessorFromUUID(wiring.getSourceId()));
                }
            }
        }

        /*for (NifiRPG rpg : rpgsToRemove) {
            /*Device device = getDeviceAndRpg(rpg.getId(), iRPGMap).getLeft();
            for (ActualWiring wiring : iWiringMap.get(device)) {
                for (Processor processor : iProcessorMap.get(device)) {
                    if (Integer.parseInt(processor.getUuid()) == wiring.getSourceId() &&
                            !processorsToMove.contains(processor)) {
                        processorsToPause.add(processor);
                        break;
                    }
                }
            }
            for (ActualWiring wiring : iGlobalWiring) {
                if (wiring.getSourceType().equals(ActualWiring.RPG)
                        && wiring.getSourceId() == rpg.getId()) {
                    portsToRemove.add(getDeviceAndPort(wiring.getDestinationId(), iPortMap).getRight());
                    break;
                } else if (wiring.getDestinationType().equals(ActualWiring.RPG)
                        && wiring.getDestinationId() == rpg.getId()) {
                    portsToRemove.add(getDeviceAndPort(wiring.getSourceId(), iPortMap).getRight());
                    break;
                }
            }
        }*/
        for (NifiKafkaPort kafkaPort : kafkaPortsToRemove) {
            Device device = getDeviceAndKafkaPort(kafkaPort.getId(), iKafkaMap).getLeft();
            for (ActualWiring wiring : iWiringMap.get(device)) {
                if (wiring.getSourceType().equals(ActualWiring.KAFKA_PORT)
                        && wiring.getSourceId() == kafkaPort.getId()
                        && wiring.getDestinationType().equals(ActualWiring.PROCESSOR)) {
                    for (Processor processor : iProcessorMap.get(device)) {
                        if (Integer.parseInt(processor.getUuid()) == wiring.getSourceId() &&
                                !processorsToMove.contains(processor)) {
                            processorsToPause.add(processor);
                            break;
                        }
                    }
                } else if (wiring.getDestinationType().equals(ActualWiring.KAFKA_PORT)
                        && wiring.getDestinationId() == kafkaPort.getId()
                        && wiring.getSourceType().equals(ActualWiring.PROCESSOR)) {
                    for (Processor processor : iProcessorMap.get(device)) {
                        if (Integer.parseInt(processor.getUuid()) == wiring.getSourceId() &&
                                !processorsToMove.contains(processor)) {
                            processorsToPause.add(processor);
                            break;
                        }
                    }
                }
            }
        }

        Pair<Set<Processor>, Set<NifiCommPort>> left = new ImmutablePair<>(processorsToPause, portsToRemove);
        Pair<Set<NifiRPG>, Set<NifiKafkaPort>> right = new ImmutablePair<>(rpgsToRemove, kafkaPortsToRemove);
        return new ImmutablePair<>(left, right);
    }

    private Set<Processor> findMappingDiff(Map<Processor, Device> newPlacementMap) {
        Set<Processor> diffList = new HashSet<>();
        for (Map.Entry<Processor, Device> entry : newPlacementMap.entrySet()) {
            Processor processor = entry.getKey();

            if(! processorMapping.get(processor).equals(entry.getValue())) {
                System.out.println(processorMapping.get(processor).getDeviceIP() + " vs " + entry.getValue().getDeviceIP());
                diffList.add(processor);
            }

        }

        return diffList;
    }

    public boolean stopDag() throws Exception {
        try {
            mqttClient.connect();
            stopArtifactsAndPurgeConnections();
            removeArtifacts();
            mqttClient.disconnect();
        } catch(MqttException e ) {
            e.printStackTrace();
        }
        // stop all processors and ports and rpgs, purge all connections.
        // remove all connections.
        // remove all processors and ports and rpgs.
        return true;
    }

    private void removeArtifacts() throws Exception {
        int datagramCount = 0;
        for (Map.Entry<Device, List<Processor>> entry : this.iProcessorMap.entrySet()) {
            Device device = entry.getKey();
            String mqttTopic = device.getDeviceUUID();
            ControlDatagram datagram = new ControlDatagram();
            datagram.setAckTopic(sessionId);
            datagram.setSessionId(sessionId);
            datagram.setResourceId(device.getDeviceUUID());
            int sequence = 1;
            for (Processor processor : entry.getValue()) {
                ControlMethod method = new ControlMethod();
                method.setMethodName("remove_processor");
                method.setSequenceId(sequence++);
                Map<String, String> params = new HashMap<>();
                params.put("proc_id", processor.getNifiId());
                method.setParams(params);
                datagram.addMethod(method);
            }
            for (NifiCommPort port : this.iPortMap.get(device)) {
                ControlMethod method = new ControlMethod();
                if (port.isInput())
                    method.setMethodName("remove_input_port");
                else
                    method.setMethodName("remove_output_port");
                method.setSequenceId(sequence++);
                Map<String, String> params = new HashMap<>();
                params.put("port_id", port.getNifiId());
                method.setParams(params);
                datagram.addMethod(method);
            }
            for (NifiRPG rpg : this.iRPGMap.get(device)) {
                ControlMethod method = new ControlMethod();
                method.setMethodName("remove_rpg");
                method.setSequenceId(sequence++);
                Map<String, String> params = new HashMap<>();
                params.put("rpg_id", rpg.getNifiId());
                method.setParams(params);
                datagram.addMethod(method);
            }
            for (NifiKafkaPort kafkaport : this.iKafkaMap.get(device)) {
                ControlMethod method = new ControlMethod();
                method.setMethodName("remove_kafka_port");
                method.setSequenceId(sequence++);
                Map<String, String> params = new HashMap<>();
                params.put("port_id", kafkaport.getNifiId());
                method.setParams(params);
                datagram.addMethod(method);
            }
            String payloadJson = "";
            try {
                ObjectMapper objectMapper = new ObjectMapper();
                payloadJson = objectMapper.writeValueAsString(datagram);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            MqttMessage message = new MqttMessage(payloadJson.getBytes());
            message.setQos(2);
            try {
                mqttClient.publish(mqttTopic, message);
            } catch (MqttException e) {
                e.printStackTrace();
            }
            datagramCount++;
        }
        Map<Integer, ResponseDatagram> responses =
                ControlResponseReceiver.receiveResponse(datagramCount, sessionId, mqttClient.getServerURI());
    }

    private void stopArtifactsAndPurgeConnections() throws Exception {
        int datagramCount = 0;
        for (Map.Entry<Device, List<Processor>> entry : this.iProcessorMap.entrySet()) {
            Device device = entry.getKey();
            String mqttTopic = device.getDeviceUUID();
            ControlDatagram datagram = new ControlDatagram();
            datagram.setAckTopic(sessionId);
            datagram.setSessionId(sessionId);
            datagram.setResourceId(device.getDeviceUUID());
            int sequence = 1;
            for (Processor processor : entry.getValue()) {
                ControlMethod method = new ControlMethod();
                method.setMethodName("stop_processor");
                method.setSequenceId(sequence++);
                Map<String, String> params = new HashMap<>();
                params.put("proc_id", processor.getNifiId());
                method.setParams(params);
                datagram.addMethod(method);
            }
            for (NifiCommPort port : this.iPortMap.get(device)) {
                ControlMethod method = new ControlMethod();
                if (port.isInput())
                    method.setMethodName("disable_input_port");
                else
                    method.setMethodName("disable_output_port");
                method.setSequenceId(sequence++);
                Map<String, String> params = new HashMap<>();
                params.put("port_id", port.getNifiId());
                method.setParams(params);
                datagram.addMethod(method);
            }
            for (NifiKafkaPort kafkaPort : this.iKafkaMap.get(device)) {
                ControlMethod method = new ControlMethod();
                method.setMethodName("disable_kafka_port");
                method.setSequenceId(sequence++);
                Map<String, String> params = new HashMap<>();
                params.put("port_id", kafkaPort.getNifiId());
                method.setParams(params);
                datagram.addMethod(method);
            }
            for (NifiRPG rpg : this.iRPGMap.get(device)) {
                ControlMethod method = new ControlMethod();
                method.setMethodName("disable_rpg");
                method.setSequenceId(sequence++);
                Map<String, String> params = new HashMap<>();
                params.put("rpg_id", rpg.getNifiId());
                method.setParams(params);
                datagram.addMethod(method);
            }
            for (ActualWiring wiring : this.iWiringMap.get(device)) {
                ControlMethod method = new ControlMethod();
                method.setMethodName("purge_connection");
                method.setSequenceId(sequence++);
                Map<String, String> params = new HashMap<>();
                params.put("connection_id", wiring.getNifiId());
                method.setParams(params);
                datagram.addMethod(method);
            }
            String payloadJson = "";
            try {
                ObjectMapper objectMapper = new ObjectMapper();
                payloadJson = objectMapper.writeValueAsString(datagram);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            MqttMessage message = new MqttMessage(payloadJson.getBytes());
            message.setQos(2);
            try {
                mqttClient.publish(mqttTopic, message);
            } catch (MqttException e) {
                e.printStackTrace();
            }
            datagramCount++;
        }
        Map<Integer, ResponseDatagram> responses =
                ControlResponseReceiver.receiveResponse(datagramCount, sessionId, mqttClient.getServerURI());
    }

    private Map<Device, List<Processor>> getProcessorMap(Map<Processor, Device> placementMap) {

        Map<Device, List<Processor>> deviceSetList = new HashMap<>();
        for (Map.Entry<Processor, Device> entry : placementMap.entrySet()) {
            Device currentDevice = entry.getValue();
            Processor currentProcessor = entry.getKey();
            if (deviceSetList.containsKey(currentDevice)) {
                List<Processor> processors = new ArrayList<>(deviceSetList.get(currentDevice));
                processors.add(currentProcessor);
                deviceSetList.put(currentDevice, processors);
            } else {
                List<Processor> processors = new ArrayList<>();
                processors.add(currentProcessor);
                deviceSetList.put(currentDevice, processors);
            }
            System.out.println(entry.getKey().getName() + " " + entry.getValue().getDeviceIP());
        }
        return deviceSetList;
    }

    private Map<Device, List<NifiCommPort>> addNewPort(Device device, NifiCommPort port,
                                                       Map<Device, List<NifiCommPort>> devicePortMap) {
        List<NifiCommPort> ports;

        if (devicePortMap.containsKey(device)) {
            ports = devicePortMap.get(device);
        } else {
            ports = new ArrayList<>();
        }
        ports.add(port);
        devicePortMap.put(device, ports);

        return devicePortMap;

    }

    private Map<Device, List<NifiRPG>> addNewRPG(Device device, NifiRPG rpg,
                                                 Map<Device, List<NifiRPG>> deviceRpgMap) {
        List<NifiRPG> rpgs;

        if (deviceRpgMap.containsKey(device)) {
            rpgs = deviceRpgMap.get(device);
        } else {
            rpgs = new ArrayList<>();
        }
        rpgs.add(rpg);
        deviceRpgMap.put(device, rpgs);

        return deviceRpgMap;
    }

    private Map<Device, List<NifiKafkaPort>> addNewKafkaPort(Device device, NifiKafkaPort kafkaPort,
                                                             Map<Device, List<NifiKafkaPort>> deviceKafkaMap) {
        List<NifiKafkaPort> ports;

        if (deviceKafkaMap.containsKey(device)) {
            ports = deviceKafkaMap.get(device);
        } else {
            ports = new ArrayList<>();
        }
        ports.add(kafkaPort);
        deviceKafkaMap.put(device, ports);

        return deviceKafkaMap;
    }

    private Map<Device, List<ActualWiring>> addNewWiring(Device device, ActualWiring newWiring,
                                                         Map<Device, List<ActualWiring>> deviceWiringMap) {
        List<ActualWiring> actualWirings;

        if (deviceWiringMap.containsKey(device)) {
            actualWirings = deviceWiringMap.get(device);
        } else {
            actualWirings = new ArrayList<>();
        }
        actualWirings.add(newWiring);
        deviceWiringMap.put(device, actualWirings);

        return deviceWiringMap;
    }


    private void populateMaps(Set<Wiring> wiring, Map<Processor, Device> placementMap) {
        random.setSeed(System.currentTimeMillis());
        Map<Device, List<NifiCommPort>> devicePortMap = new HashMap<>();
        Map<Device, List<NifiRPG>> deviceRPGMap = new HashMap<>();
        Map<Device, List<ActualWiring>> deviceWiringMap = new HashMap<>();
        Map<Device, List<NifiKafkaPort>> deviceKafkaMap = new HashMap<>();
        Set<ActualWiring> globalWiring = new HashSet<>();
        int portCounter = 1;
        int rpgCounter = 1;
        int kafkaCounter = 1;

        for (Wiring wire : wiring) {
            String relationship = new String();
            if (!wire.getSelectedRelationships().isEmpty())
                relationship = wire.getSelectedRelationships().iterator().next();

            String source = wire.getSourceProcessor(), destination = wire.getDestinationProcessor(); // IDs
            Processor sourceProcessor = null, destinationProcessor = null;
            Device sourceDevice = null, destinationDevice = null;

            for (Map.Entry<Processor, Device> entry : placementMap.entrySet()) {
                if (sourceProcessor == null && entry.getKey().getUuid().equals(source)) {
                    sourceProcessor = entry.getKey();
                    sourceDevice = entry.getValue();
                } else if (destinationProcessor == null && entry.getKey().getUuid().equals(destination)) {
                    destinationProcessor = entry.getKey();
                    destinationDevice = entry.getValue();
                }
            }

            int sourceId = Integer.parseInt(sourceProcessor.getUuid()),
                    destinationId = Integer.parseInt(destinationProcessor.getUuid());

            if (sourceDevice.equals(destinationDevice)) {
                ActualWiring newWiring = new ActualWiring(
                        ActualWiring.PROCESSOR, sourceId, ActualWiring.PROCESSOR, destinationId, relationship);
                deviceWiringMap = addNewWiring(sourceDevice, newWiring, deviceWiringMap);

            } else {
                String direction = matrix.getDirection(
                        Integer.parseInt(sourceDevice.getDeviceUUID()),
                        Integer.parseInt(destinationDevice.getDeviceUUID()));

                if (!direction.equals(NetworkVisibilityMatrix.NOT_VISIBLE)) {
                    NifiRPG rpg;
                    NifiCommPort port;

                    if (direction.equals(NetworkVisibilityMatrix.PUSH_DIRECTION)) {
                        rpg = new NifiRPG(rpgCounter++, sourceId, destinationDevice.getUrl(), relationship);
                        port = new NifiCommPort(true, portCounter++);
                        ActualWiring newWiring = new ActualWiring(
                                ActualWiring.INPUT_PORT, port.getId(), ActualWiring.PROCESSOR,
                                destinationId, null);

                        deviceRPGMap = addNewRPG(sourceDevice, rpg, deviceRPGMap);
                        devicePortMap = addNewPort(destinationDevice, port, devicePortMap);
                        deviceWiringMap = addNewWiring(destinationDevice, newWiring, deviceWiringMap);
                        globalWiring.add(new ActualWiring(
                                ActualWiring.RPG, rpg.getId(), ActualWiring.INPUT_PORT, port.getId(), null));
                    } else {
                        rpg = new NifiRPG(rpgCounter++, destinationId, sourceDevice.getUrl(), null);
                        port = new NifiCommPort(false, portCounter++);
                        ActualWiring newWiring = new ActualWiring(
                                ActualWiring.PROCESSOR, sourceId, ActualWiring.OUTPUT_PORT,
                                port.getId(), relationship);

                        devicePortMap = addNewPort(sourceDevice, port, devicePortMap);
                        deviceRPGMap = addNewRPG(destinationDevice, rpg, deviceRPGMap);
                        deviceWiringMap = addNewWiring(sourceDevice, newWiring, deviceWiringMap);
                        globalWiring.add(new ActualWiring(
                                ActualWiring.OUTPUT_PORT, port.getId(), ActualWiring.RPG, rpg.getId(), null));
                    }
                } else {
                    // random topic name
                    String topic = UUID.randomUUID().toString();
                    NifiKafkaPort publisher = new NifiKafkaPort(false, kafkaCounter++, topic);
                    NifiKafkaPort consumer = new NifiKafkaPort(true, kafkaCounter++, topic);

                    deviceWiringMap = addNewWiring(sourceDevice, new ActualWiring(
                            ActualWiring.PROCESSOR, sourceId, ActualWiring.KAFKA_PORT, publisher.getId(), relationship
                    ), deviceWiringMap);
                    deviceWiringMap = addNewWiring(destinationDevice, new ActualWiring(
                            ActualWiring.KAFKA_PORT, consumer.getId(), ActualWiring.PROCESSOR, destinationId, "success"
                    ), deviceWiringMap);
                    globalWiring.add(new ActualWiring(
                            ActualWiring.KAFKA_PORT, publisher.getId(), ActualWiring.KAFKA_PORT, consumer.getId(), null));

                    deviceKafkaMap = addNewKafkaPort(sourceDevice, publisher, deviceKafkaMap);
                    deviceKafkaMap = addNewKafkaPort(destinationDevice, consumer, deviceKafkaMap);
                }
            }
        }
        iProcessorMap = getProcessorMap(placementMap);
        iPortMap = devicePortMap;
        iRPGMap = deviceRPGMap;
        iWiringMap = deviceWiringMap;
        iGlobalWiring = globalWiring;
        iKafkaMap = deviceKafkaMap;
    }

    private void logMaps(Map<Device, Set<Processor>> deviceSetMap,
                         Map<Device, Set<NifiCommPort>> portMap,
                         Map<Device, Set<NifiRPG>> rpgMap,
                         Map<Device, Set<ActualWiring>> wiringMap,
                         Set<ActualWiring> globalWiring) {
        System.out.println("--Processor Map--------------\n");
        for (Map.Entry<Device, Set<Processor>> entry : deviceSetMap.entrySet()) {
            System.out.println("For Device " + entry.getKey().getDeviceIP());
            for (Processor processor : entry.getValue()) {
                System.out.println(processor.getUuid());
            }
            System.out.println();
        }
        System.out.println("--Port Map--------------\n");

        for (Map.Entry<Device, Set<NifiCommPort>> entry : portMap.entrySet()) {
            System.out.println("For Device " + entry.getKey().getDeviceIP());
            for (NifiCommPort commPort : entry.getValue()) {
                System.out.println(commPort.getId());
            }
            System.out.println();
        }
        System.out.println("--RPG Map--------------\n");

        for (Map.Entry<Device, Set<NifiRPG>> entry : rpgMap.entrySet()) {
            System.out.println("For Device " + entry.getKey().getDeviceIP());
            for (NifiRPG rpg : entry.getValue()) {
                System.out.println(rpg.getId() + " " + rpg.getProcessorId() + " " + rpg.getRelationship());
            }
            System.out.println();
        }
        System.out.println("--Wiring Map--------------\n");

        for (Map.Entry<Device, Set<ActualWiring>> entry : wiringMap.entrySet()) {
            System.out.println("For Device " + entry.getKey().getDeviceIP());
            for (ActualWiring wiring : entry.getValue()) {
                System.out.println(wiring.getSourceType() + " "
                        + wiring.getSourceId() + " " + wiring.getDestinationType() + " "
                        + wiring.getDestinationId() + " " + wiring.getRelationship());
            }
            System.out.println();
        }
        System.out.println("--Global Wiring--------------\n");
        for (ActualWiring wiring : globalWiring) {
            System.out.println(wiring.getSourceType() + " "
                    + wiring.getSourceId() + " " + wiring.getDestinationType() + " "
                    + wiring.getDestinationId() + " " + wiring.getRelationship());
        }

    }

    private Map<Integer, Device> getResourceIdMap(Set<Device> devices) {
        Map<Integer, Device> resourceIdMap = new HashMap<>();
        for (Device device : devices) {
            resourceIdMap.put(Integer.parseInt(device.getDeviceUUID()), device);
        }
        return resourceIdMap;
    }

    private Map<Integer, Device> getResourceIdMap() {
        Map<Integer, Device> resourceIdMap = new HashMap<>();
        for (Map.Entry<Device, List<Processor>> entry : iProcessorMap.entrySet()) {
            Device device = entry.getKey();
            resourceIdMap.put(Integer.parseInt(device.getDeviceUUID()), device);
        }
        return resourceIdMap;
    }

    public DataflowInput deployDag(Map<Processor, Device> placementMap,
                                   DataflowInput input) throws Exception {

        this.processorMapping = placementMap;
        for (Map.Entry<Processor, Device> entry : this.processorMapping.entrySet()) {
            System.out.println(entry.getValue().getDeviceIP());
        }

        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setCleanSession(true);
        try {
            mqttClient.connect();

            populateMaps(input.getWiring(), placementMap);

            createProcessorsPortsAndRPGs();
            createConnections();
            startAllPorts();
            createRemoteConnections();
            startAllProcessors();

            mqttClient.disconnect();
        } catch (MqttException e) {
            e.printStackTrace();
        }
        return input;
    }

    private void startAllProcessors() throws Exception {
        int datagramCount = 0;
        for (Map.Entry<Device, List<Processor>> entry : iProcessorMap.entrySet()) {
            String mqttTopic = entry.getKey().getDeviceUUID();
            ControlDatagram datagram = new ControlDatagram();
            datagram.setAckTopic(sessionId);
            datagram.setSessionId(sessionId);
            datagram.setResourceId(entry.getKey().getDeviceUUID());
            int sequence = 1;
            for (Processor processor : entry.getValue()) {
                ControlMethod method = new ControlMethod();
                method.setMethodName("start_processor");
                method.setSequenceId(sequence++);
                Map<String, String> params = new HashMap<>();
                params.put("proc_id", processor.getNifiId());
                method.setParams(params);
                datagram.addMethod(method);
            }
            String payloadJson = "";
            try {
                ObjectMapper objectMapper = new ObjectMapper();
                payloadJson = objectMapper.writeValueAsString(datagram);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            System.out.println(payloadJson);
            // publish to Mqtt
            MqttMessage message = new MqttMessage(payloadJson.getBytes());
            message.setQos(2);
            mqttClient.publish(mqttTopic, message);
            datagramCount++;
        }
        Map<Integer, ResponseDatagram> responses =
                ControlResponseReceiver.receiveResponse(datagramCount, sessionId, mqttClient.getServerURI());
    }

    private void startAllPorts() throws Exception {
        int datagramCount = 0;
        for (Map.Entry<Device, List<NifiCommPort>> entry : iPortMap.entrySet()) {
            String mqttTopic = entry.getKey().getDeviceUUID();
            ControlDatagram datagram = new ControlDatagram();
            datagram.setAckTopic(sessionId);
            datagram.setSessionId(sessionId);
            datagram.setResourceId(entry.getKey().getDeviceUUID());
            int sequence = 1;
            for (NifiCommPort port : entry.getValue()) {
                if (!port.isInput())
                    continue;
                ControlMethod method = new ControlMethod();
                method.setMethodName("enable_input_port");
                method.setSequenceId(sequence++);
                Map<String, String> params = new HashMap<>();
                params.put("port_id", port.getNifiId());
                method.setParams(params);
                datagram.addMethod(method);
            }
            for (NifiKafkaPort kafkaPort : iKafkaMap.get(entry.getKey())) {
                ControlMethod method = new ControlMethod();
                method.setMethodName("enable_kafka_port");
                method.setSequenceId(sequence++);
                Map<String, String> params = new HashMap<>();
                params.put("port_id", kafkaPort.getNifiId());
                method.setParams(params);
                datagram.addMethod(method);
            }
            if (sequence == 1)
                continue;
            String payloadJson = "";
            try {
                ObjectMapper objectMapper = new ObjectMapper();
                payloadJson = objectMapper.writeValueAsString(datagram);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            System.out.println(payloadJson);
            // publish to Mqtt
            MqttMessage message = new MqttMessage(payloadJson.getBytes());
            message.setQos(2);
            mqttClient.publish(mqttTopic, message);
            datagramCount++;
        }
        Map<Integer, ResponseDatagram> responses =
                ControlResponseReceiver.receiveResponse(datagramCount, sessionId, mqttClient.getServerURI());
    }

    private void createRemoteConnections() throws Exception {

        int datagramCount = 0;
        for (ActualWiring wiring : iGlobalWiring) {
            if (wiring.getSourceType() == ActualWiring.KAFKA_PORT)
                continue;
            String sourceType = wiring.getSourceType();
            ControlDatagram datagram = new ControlDatagram();
            datagram.setSessionId(sessionId);
            datagram.setAckTopic(sessionId);
            ControlMethod method = new ControlMethod();
            method.setSequenceId(1);
            int rpgId;
            int portId;
            if (sourceType.equals(ActualWiring.RPG)) {
                // input port case
                rpgId = wiring.getSourceId();
                portId = wiring.getDestinationId();
                method.setMethodName("connect_remote_input_port");
            } else {
                // output port case
                portId = wiring.getSourceId();
                rpgId = wiring.getDestinationId();
                method.setMethodName("connect_remote_output_port");
            }
            Map<String, String> params = new HashMap<>();
            // get device for rpg
            // get NifiId for rpg
            Pair<Device, NifiRPG> pair = getDeviceAndRpg(rpgId, iRPGMap);
            Device device = pair.getLeft();
            NifiRPG rpg = pair.getRight();
            params.put("rpg", rpg.getNifiId());
            params.put("port_id", getPortId(portId, iPortMap));
            params.put("proc_id", getProcId(rpg.getProcessorId(), iProcessorMap));
            params.put("relationship", rpg.getRelationship());
            // get portId for rpg
            method.setParams(params);
            datagram.setResourceId(device.getDeviceUUID());
            datagram.addMethod(method);

            datagramCount++;
            String payloadJson = "";
            try {
                ObjectMapper objectMapper = new ObjectMapper();
                payloadJson = objectMapper.writeValueAsString(datagram);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }

            MqttMessage message = new MqttMessage(payloadJson.getBytes());
            message.setQos(2);
            String mqttTopic = device.getDeviceUUID();
            mqttClient.publish(mqttTopic, message);
        }
        Map<Integer, ResponseDatagram> responses =
                ControlResponseReceiver.receiveResponse(datagramCount, sessionId, mqttClient.getServerURI());
    }

    private String getProcId(int procId, Map<Device, List<Processor>> lprocMap) {
        for (Map.Entry<Device, List<Processor>> entry : lprocMap.entrySet()) {
            for (Processor proc : entry.getValue()) {
                if (Integer.parseInt(proc.getUuid()) == procId) {
                    return proc.getNifiId();
                }
            }
        }
        return null;
    }

    private String getPortId(int portId, Map<Device, List<NifiCommPort>> lportMap) {
        for (Map.Entry<Device, List<NifiCommPort>> entry : lportMap.entrySet()) {
            for (NifiCommPort port : entry.getValue()) {
                if (port.getId() == portId) {
                    return port.getNifiId();
                }
            }
        }
        return null;
    }

    private Pair<Device, NifiKafkaPort> getDeviceAndKafkaPort(int portId, Map<Device, List<NifiKafkaPort>> lKafkaPortMap) {
        for (Map.Entry<Device, List<NifiKafkaPort>> entry : lKafkaPortMap.entrySet()) {
            for (NifiKafkaPort nifiKafkaPort : entry.getValue()) {
                if (nifiKafkaPort.getId() == portId) {
                    return new ImmutablePair<>(entry.getKey(), nifiKafkaPort);
                }
            }
        }
        return null;
    }

    private Pair<Device, NifiCommPort> getDeviceAndPort(int portId, Map<Device, List<NifiCommPort>> lportMap) {
        for (Map.Entry<Device, List<NifiCommPort>> entry : lportMap.entrySet()) {
            for (NifiCommPort port : entry.getValue()) {
                if (port.getId() == portId) {
                    return new ImmutablePair<>(entry.getKey(), port);
                }
            }
        }
        return null;
    }

    private Pair<Device, NifiRPG> getDeviceAndRpg(int rpgId, Map<Device, List<NifiRPG>> lrpgMap) {
        for (Map.Entry<Device, List<NifiRPG>> entry : lrpgMap.entrySet()) {
            for (NifiRPG rpg : entry.getValue()) {
                if (rpg.getId() == rpgId) {
                    return new ImmutablePair<>(entry.getKey(), rpg);
                }
            }
        }
        return null;
    }

    private void createConnections() throws Exception {

        String ackTopic = sessionId;
        Map<Integer, Device> resourceIdMap = getResourceIdMap();
        int datagramCount = 0;
        for (Map.Entry<Device, List<ActualWiring>> entry : iWiringMap.entrySet()) {
            Device device = entry.getKey();
            String mqttTopic = device.getDeviceUUID();
            List<ActualWiring> wirings = entry.getValue();
            ControlDatagram datagram = new ControlDatagram();
            datagram.setResourceId(device.getDeviceUUID());
            datagram.setAckTopic(ackTopic);
            datagram.setSessionId(sessionId);

            int sequence = 1;
            for (ActualWiring wiring : wirings) {
                ControlMethod method = new ControlMethod();
                method.setMethodName("create_connection");
                method.setSequenceId(sequence++);

                String sourceNifiId = getNifiId(
                        wiring.getSourceId(), wiring.getSourceType(), iProcessorMap.get(device), iPortMap.get(device),
                        iRPGMap.get(device), iKafkaMap.get(device));
                String destinationNifiId = getNifiId(
                        wiring.getDestinationId(), wiring.getDestinationType(), iProcessorMap.get(device), iPortMap.get(device),
                        iRPGMap.get(device), iKafkaMap.get(device));
                Map<String, String> params = new HashMap<>();
                params.put("from_id", sourceNifiId);
                params.put("to_id", destinationNifiId);
                params.put("relationship_name", wiring.getRelationship());

                method.setParams(params);
                datagram.addMethod(method);
            }
            if (datagram.getMethodSet().size() == 0)
                continue;
            datagramCount++;
            String payloadJson = "";

            try {
                ObjectMapper objectMapper = new ObjectMapper();
                payloadJson = objectMapper.writeValueAsString(datagram);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }

            MqttMessage message = new MqttMessage(payloadJson.getBytes());
            message.setQos(2);
            mqttClient.publish(mqttTopic, message);
        }
        Map<Integer, ResponseDatagram> responses =
                ControlResponseReceiver.receiveResponse(datagramCount, ackTopic, mqttClient.getServerURI());
        for (Map.Entry<Integer, ResponseDatagram> entry : responses.entrySet()) {
            Integer resourceId = entry.getKey();
            Device device = resourceIdMap.get(resourceId);
            ResponseDatagram response = entry.getValue();
            Map<String, ControlResponse> responseMap = response.getResponseSet();
            int i = 1;
            for (ActualWiring wiring : iWiringMap.get(device)) {
                String id = Integer.toString(i);
                String nifiId = responseMap.get(id).getId();
                wiring.setNifiId(nifiId);
                i++;
            }
        }
    }

    private String getNifiId(int id, String type, List<Processor> processors,
                             List<NifiCommPort> ports, List<NifiRPG> rpgs, List<NifiKafkaPort> kafkaPorts) {
        String nifiId = null;
        if (type.equals(ActualWiring.PROCESSOR)) {
            for (Processor processor : processors)
                if (Integer.parseInt(processor.getUuid()) == id)
                    nifiId = processor.getNifiId();
        } else if (type.equals(ActualWiring.RPG)) {
            for (NifiRPG rpg : rpgs)
                if (rpg.getId() == id)
                    nifiId = rpg.getNifiId();

        } else if (type.equals(ActualWiring.INPUT_PORT) || type.equals(ActualWiring.OUTPUT_PORT)) {
            for (NifiCommPort port : ports)
                if (port.getId() == id)
                    nifiId = port.getNifiId();

        } else {
            for (NifiKafkaPort port : kafkaPorts)
                if (port.getId() == id)
                    nifiId = port.getNifiId();
        }
        return nifiId;
    }

    private void createProcessorsPortsAndRPGs() throws Exception {
        String ackTopic = sessionId;

        Map<Integer, Device> resourceIdMap = getResourceIdMap();

        for (Map.Entry<Device, List<Processor>> entry : iProcessorMap.entrySet()) {

            Device device = entry.getKey();
            String resourceId = device.getDeviceUUID();
            String mqttTopic = resourceId;
            ControlDatagram datagram = new ControlDatagram();
            datagram.setResourceId(resourceId);
            datagram.setSessionId(sessionId);
            datagram.setAckTopic(ackTopic);

            List<Processor> processors = entry.getValue();
            List<NifiCommPort> ports = new ArrayList<>();
            List<NifiRPG> rpgs = new ArrayList<>();
            List<NifiKafkaPort> kafkaPorts = new ArrayList<>();

            if (iPortMap.get(device) != null)
                ports = iPortMap.get(device);
            if (iRPGMap.get(device) != null)
                rpgs = iRPGMap.get(device);
            if (iKafkaMap.get(device) != null)
                kafkaPorts = iKafkaMap.get(device);

            iPortMap.put(device, ports);
            iRPGMap.put(device, rpgs);
            iKafkaMap.put(device, kafkaPorts);

            int sequence = 1;

            for (Processor processor : processors) {

                ControlMethod method = new ControlMethod();
                Map<String, String> params = new HashMap<>();

                method.setMethodName("create_processor");
                method.setSequenceId(sequence++);
                params.put("class", processor.getClass_());
                params.put("name", processor.getName());

                String propertiesJson = "";
                String relationshipsJson = "";
                String configsJson = "";
                Map<String, Object> properties = new HashMap<>();
                for (Property p : processor.getProperties())
                    for (Map.Entry<String, Object> P : p.getAdditionalProperties().entrySet())
                        properties.put(P.getKey(), P.getValue());

                Map<String, Object> configs = new HashMap<>();
                for (Config c : processor.getConfig())
                    for (Map.Entry<String, Object> C : c.getAdditionalProperties().entrySet())
                        configs.put(C.getKey(), C.getValue());

                ObjectMapper mapper = new ObjectMapper();
                try {
                    propertiesJson = mapper.writeValueAsString(properties);
                    relationshipsJson = mapper.writeValueAsString(processor.getRelationships());
                    configsJson = mapper.writeValueAsString(configs);
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
                params.put("properties", propertiesJson);
                params.put("relationships", relationshipsJson);
                params.put("configs", configsJson);

                method.setParams(params);
                datagram.addMethod(method);
                // create processor
            }
            for (NifiCommPort port : ports) {
                // create port
                ControlMethod method = new ControlMethod();
                String methodName;
                if (port.isInput())
                    methodName = "create_input_port";
                else
                    methodName = "create_output_port";
                Map<String, String> params = new HashMap<>();
                params.put("name", sessionId + " " + Integer.toString(port.getId()));
                method.setMethodName(methodName);
                method.setSequenceId(sequence++);
                method.setParams(params);
                datagram.addMethod(method);
            }
            for (NifiRPG rpg : rpgs) {
                // create rpg
                ControlMethod method = new ControlMethod();
                method.setMethodName("create_rpg");
                method.setSequenceId(sequence++);
                Map<String, String> params = new HashMap<>();
                params.put("url", rpg.getRemoteUrl());
                method.setParams(params);
                datagram.addMethod(method);
            }
            for (NifiKafkaPort kafkaPort : kafkaPorts) {
                ControlMethod method = new ControlMethod();
                if (kafkaPort.isConsumer())
                    method.setMethodName("create_kafka_consumer");
                else
                    method.setMethodName("create_kafka_producer");
                method.setSequenceId(sequence++);
                Map<String, String> params = new HashMap<>();
                params.put("topic", kafkaPort.getTopic());
                method.setParams(params);
                datagram.addMethod(method);
            }

            String payloadJson = "";
            try {
                ObjectMapper objectMapper = new ObjectMapper();
                payloadJson = objectMapper.writeValueAsString(datagram);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            System.out.println(payloadJson);
            // publish to Mqtt
            MqttMessage message = new MqttMessage(payloadJson.getBytes());
            message.setQos(2);
            mqttClient.publish(mqttTopic, message);
        }
        Map<Integer, ResponseDatagram> responses =
                ControlResponseReceiver.receiveResponse(iProcessorMap.size(), ackTopic, mqttClient.getServerURI());
        for (Map.Entry<Integer, ResponseDatagram> entry : responses.entrySet()) {
            Integer resourceId = entry.getKey();
            Device device = resourceIdMap.get(resourceId);
            ResponseDatagram response = entry.getValue();
            Map<String, ControlResponse> responseMap = response.getResponseSet();
            int i = 1;
            for (Processor processor : iProcessorMap.get(device)) {
                String id = Integer.toString(i);
                String nifiId = responseMap.get(id).getId();
                processor.setNifiId(nifiId);
                i++;
            }
            for (NifiCommPort port : iPortMap.get(device)) {
                String id = Integer.toString(i);
                String nifiId = responseMap.get(id).getId();
                port.setNifiId(nifiId);
                i++;
            }
            for (NifiRPG rpg : iRPGMap.get(device)) {
                String id = Integer.toString(i);
                String nifiId = responseMap.get(id).getId();
                rpg.setNifiId(nifiId);
                i++;
            }
            for (NifiKafkaPort kafkaPort : iKafkaMap.get(device)) {
                String id = Integer.toString(i);
                String nifiId = responseMap.get(id).getId();
                kafkaPort.setNifiId(nifiId);
                i++;
            }
        }
    }

}

