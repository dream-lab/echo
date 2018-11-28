package in.dream_lab.echo.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import in.dream_lab.echo.utils.ControlResponse;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import javax.xml.ws.Response;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Created by pushkar on 2/9/17.
 */
public class ControlResponseReceiver implements MqttCallback{
    String topicName;
    String broker;
    int count;
    int recCount;
    Map<Integer, ResponseDatagram> responseSet;

    public ControlResponseReceiver(String broker, String topicName, int count) {
        this.topicName = topicName;
        this.broker = broker;
        this.count = count;
        this.responseSet = new HashMap<>();
        this.recCount = 0;
    }

    @Override
    public void connectionLost(Throwable t) {
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        //System.out.println("Pub complete" + new String(token.getMessage().getPayload()));
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        System.out.println(new String(message.getPayload()));
        ResponseDatagram response = mapper.readValue(new String(message.getPayload()),
                ResponseDatagram.class);
        responseSet.put(Integer.parseInt(response.getResourceId()), response);
        this.recCount++;
    }

    public void run() {
        MqttConnectOptions connOpt = new MqttConnectOptions();
        connOpt.setCleanSession(false);
        try {
            System.out.println(this.broker);
            System.out.println("----");
            MemoryPersistence persistence = new MemoryPersistence();
            MqttClient client = new MqttClient(this.broker, UUID.randomUUID().toString(), persistence);
            client.setCallback(this);
            client.connect(connOpt);
            client.subscribe(this.topicName, 2);
            while (this.recCount < this.count)
                Thread.sleep(10);
            client.disconnect();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    public static Map<Integer, ResponseDatagram> receiveResponse(int count, String topicName, String broker) throws Exception{
        ControlResponseReceiver receiver = new ControlResponseReceiver(broker, topicName, count);
        System.out.println("topicName " + topicName);
        System.out.println("count = " + count);
        receiver.run();
        for (Map.Entry<Integer, ResponseDatagram> entry : receiver.responseSet.entrySet()) {
            Integer resourceId = entry.getKey();
            for (ControlResponse response : entry.getValue().getResponseSet().values()) {
                if (response.getType().equals("RetryError"))
                    throw new Exception("received RetryError from platform service");
            }
        }
        return receiver.responseSet;
    }

}
