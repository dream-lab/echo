package in.dream_lab.echo.nifi;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import in.dream_lab.echo.master.NetworkVisibilityMatrix;
import in.dream_lab.echo.utils.*;
import in.dream_lab.echo.master.AppDeployer;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import javax.sql.rowset.serial.SerialClob;
import javax.xml.ws.Response;

import java.util.*;

/**
 * Created by pushkar on 5/23/17.
 */
public class NifiDeployer implements AppDeployer {

    Random random = new Random();
    String sessionId = UUID.randomUUID().toString();
    MqttClient mqttClient;

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

    public boolean stopDag() {
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

    private void removeArtifacts() {
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

    private void stopArtifactsAndPurgeConnections() {
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

    private Map<Integer, Device> getResourceIdMap() {
        Map<Integer, Device> resourceIdMap = new HashMap<>();
        for (Map.Entry<Device, List<Processor>> entry : iProcessorMap.entrySet()) {
            Device device = entry.getKey();
            resourceIdMap.put(Integer.parseInt(device.getDeviceUUID()), device);
        }
        return resourceIdMap;
    }

    public DataflowInput deployDag(Map<Processor, Device> placementMap,
                                   DataflowInput input) {

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

    private void startAllProcessors() throws MqttException {
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

    private void startAllPorts() throws MqttException {
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

    private void createRemoteConnections() throws MqttException {

        int datagramCount = 0;
        for (ActualWiring wiring : iGlobalWiring) {
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

    private void createConnections() throws MqttException {

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

    private void createProcessorsPortsAndRPGs() throws MqttException {
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

