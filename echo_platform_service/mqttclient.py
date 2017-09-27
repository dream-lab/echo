import paho.mqtt.client as mqtt
import paho.mqtt.publish as mqttpublish
import NifiClient
import json
import time
import uuid

mqttHost = '10.24.24.222'
kafkabroker = '13.71.125.147'

def on_connect(client, userdata, flags, rc):
    print "Connected to broker"

def create_processor(params):
    client = NifiClient.NifiClient('127.0.0.1', 8080)
    relationships = params['relationships']
    configs = params['configs']
    properties = params['properties']
    name = params['name']
    class_ = params['class']

    id = client.create_processor(class_, name)
    client.set_processor_properties_and_relationships(id, properties, relationships, configs)

    return id


def create_input_port(params):
    client = NifiClient.NifiClient('127.0.0.1', 8080)
    name = params['name']
    id = None
    while id == None:
        id = client.create_new_input_port(name)
        print id
        time.sleep(3)
    return id


def enable_input_port(params):
    client = NifiClient.NifiClient('127.0.0.1', 8080)
    id = params['port_id']
    return client.enable_input_port(id)


def create_output_port(params):
    client = NifiClient.NifiClient('127.0.0.1', 8080)
    name = params['name']
    id = None
    while id == None:
        id = client.create_new_output_port(name)
        time.sleep(3)
    client.enable_output_port(id)
    return id


def create_rpg(params):
    client = NifiClient.NifiClient('127.0.0.1', 8080)
    url = params['url']
    id = None
    while id is None:
        id = client.create_remote_process_group(url)
        time.sleep(3)
    return id


def create_connection(params):
    client = NifiClient.NifiClient('127.0.0.1', 8080)
    from_id = params['from_id']
    to_id = params['to_id']
    relationship_name = params['relationship_name']
    return client.create_connection(from_id, to_id, relationship_name)


def connect_remote_input_port(params):
    client = NifiClient.NifiClient('127.0.0.1', 8080)
    rpg_id = params['rpg']
    port_id = params['port_id']
    proc_id = params['proc_id']
    relationship = params['relationship']
    id = client.connect_remote_input_port(rpg_id, port_id, proc_id, relationship)
    client.enable_rpg_transmission(rpg_id)
    return id


def connect_remote_output_port(params):
    client = NifiClient.NifiClient('127.0.0.1', 8080)
    rpg_id = params['rpg']
    port_id = params['port_id']
    proc_id = params['proc_id']
    relationship = params['relationship']
    id = client.connect_remote_output_port(rpg_id, port_id, proc_id, relationship)
    client.enable_rpg_transmission(rpg_id)
    return id


def start_processor(params):
    client = NifiClient.NifiClient('127.0.0.1', 8080)
    proc_id = params['proc_id']
    id = client.start_processor(proc_id)
    return id


def create_kafka_consumer(params):
    client = NifiClient.NifiClient('127.0.0.1', 8080)
    topic = params['topic']
    processor_class = 'in.dream_lab.echo.processors.KafkaFlowFilesConsumer'
    processor_name = 'kafka_consumer_' + topic
    id = client.create_processor(processor_class, processor_name)
    properties = dict()
    properties['topic'] = topic
    properties['group_name'] = str(uuid.uuid4())
    properties['brokerip'] = kafkabroker
    relationships = []
    relationships.append({'name': 'success', 'autoTerminate': False})
    client.set_processor_properties_and_relationships(id, json.dumps(properties),
                                                      json.dumps(relationships), json.dumps({}))
    return id


def create_kafka_producer(params):
    client = NifiClient.NifiClient('127.0.0.1', 8080)
    topic = params['topic']
    processor_class = 'in.dream_lab.echo.processors.KafkaFlowFilesProducer'
    processor_name = 'kafka_producer_' + topic
    id = client.create_processor(processor_class, processor_name)
    properties = dict()
    properties['topic'] = topic
    properties['brokerip'] = kafkabroker
    relationships = []
    relationships.append({'name': 'success', 'autoTerminate': True})
    client.set_processor_properties_and_relationships(id, json.dumps(properties),
                                                      json.dumps(relationships), json.dumps({}))
    return id


def enable_kafka_port(params):
    client = NifiClient.NifiClient('127.0.0.1', 8080)
    port_id = params['port_id']
    id = client.start_processor(port_id)
    return id


def stop_processor(params):
    client = NifiClient.NifiClient('127.0.0.1', 8080)
    processor_id = params['proc_id']
    client.stop_processor(processor_id)
    return True


def disable_input_port(params):
    client = NifiClient.NifiClient('127.0.0.1', 8080)
    port_id = params['port_id']
    client.disable_input_port(port_id)
    return True


def disable_output_port(params):
    client = NifiClient.NifiClient('127.0.0.1', 8080)
    port_id = params['port_id']
    client.disable_output_port(port_id)
    return True


def disable_kafka_port(params):
    client = NifiClient.NifiClient('127.0.0.1', 8080)
    port_id = params['port_id']
    client.stop_processor(port_id)
    return True


def disable_rpg(params):
    client = NifiClient.NifiClient('127.0.0.1', 8080)
    rpg_id = params['rpg_id']
    client.disable_rpg_transmission(rpg_id)
    adjacent_connections = client.get_connections_for_rpg(rpg_id)
    for connection in adjacent_connections:
        client.empty_connection(connection)
        client.remove_connection(connection)
    return True


def purge_connection(params):
    client = NifiClient.NifiClient('127.0.0.1', 8080)
    connection_id = params['connection_id']
    client.empty_connection(connection_id)
    client.remove_connection(connection_id)
    return True


def remove_processor(params):
    client = NifiClient.NifiClient('127.0.0.1', 8080)
    proc_id = params['proc_id']
    return client.delete_processor(proc_id)


def remove_input_port(params):
    client = NifiClient.NifiClient('127.0.0.1', 8080)
    port_id = params['port_id']
    return client.delete_input_port(port_id)


def remove_output_port(params):
    client = NifiClient.NifiClient('127.0.0.1', 8080)
    port_id = params['port_id']
    return client.delete_output_port(port_id)


def remove_rpg(params):
    client = NifiClient.NifiClient('127.0.0.1', 8080)
    rpg_id = params['rpg_id']
    return client.delete_remote_process_group(rpg_id)


def remove_kafka_port(params):
    client = NifiClient.NifiClient('127.0.0.1', 8080)
    port_id = params['port_id']
    return client.delete_processor(port_id)


def on_message(client, userdata, msg):
    print "hi"
    payload = json.loads(str(msg.payload))
    methods = payload['methodSet']
    i = 1
    datagram = dict()
    datagram['resourceId'] = payload['resourceId']
    datagram['sessionId'] = payload['sessionId']
    datagram['responseSet'] = dict()
    while str(i) in methods:
        method = methods[str(i)]
        methodName = method['methodName']
        params = method['params']
        id = None
        type = None
        if methodName == 'create_processor':
            id = create_processor(params)
            type = 'PROCESSOR'
        elif methodName == 'create_input_port':
            id = create_input_port(params)
            type = 'PORT'
        elif methodName == 'create_output_port':
            id = create_output_port(params)
            type = 'PORT'
        elif methodName == 'create_rpg':
            id = create_rpg(params)
            type = 'RPG'
        elif methodName == 'create_connection':
            id = create_connection(params)
            type = 'CONNECTION'
        elif methodName == 'connect_remote_input_port':
            id = connect_remote_input_port(params)
            type = 'CONNECTION'
        elif methodName == 'connect_remote_output_port':
            id = connect_remote_output_port(params)
            type = 'CONNECTION'
        elif methodName == 'enable_input_port':
            id = enable_input_port(params)
            type = 'PORT'
        elif methodName == 'start_processor':
            id = start_processor(params)
            type = 'PROCESSOR'
        elif methodName == 'create_kafka_consumer':
            id = create_kafka_consumer(params)
            type = 'KAFKA_PORT'
        elif methodName == 'create_kafka_producer':
            id = create_kafka_producer(params)
            type = 'KAFKA_PORT'
        elif methodName == 'enable_kafka_port':
            id = enable_kafka_port(params)
            type = 'KAFKA_PORT'
        elif methodName == 'stop_processor':
            id = stop_processor(params)
            type = 'PROCESSOR'
        elif methodName == 'disable_input_port':
            id = disable_input_port(params)
            type = 'PORT'
        elif methodName == 'disable_output_port':
            id = disable_output_port(params)
            type = 'PORT'
        elif methodName == 'disable_rpg':
            id = disable_rpg(params)
            type = 'PORT'
        elif methodName == 'purge_connection':
            id = purge_connection(params)
            type = 'PORT'
        elif methodName == 'remove_processor':
            id = remove_processor(params)
            type = 'PROCESSOR'
        elif methodName == 'remove_input_port':
            id = remove_input_port(params)
            type = 'PORT'
        elif methodName == 'remove_output_port':
            id = remove_output_port(params)
            type = 'PORT'
        elif methodName == 'remove_rpg':
            id = remove_rpg(params)
            type = 'RPG'
        elif methodName == 'remove_kafka_port':
            id = remove_kafka_port(params)
            type = 'KAFKA_PORT'
        datagram['responseSet'][str(i)] = dict([('id', id), ('type', type)])
        print methodName
        i += 1
    publish_payload = json.dumps(datagram)
    mqttpublish.single(payload['ackTopic'], publish_payload, 2, hostname = mqttHost, port = 1883,
                       client_id=payload['resourceId'])
    #print methods[0]['methodName']
    #print(msg.topic + " " + str(msg.payload))


def get_client(broker, topic):
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(broker, 1883, 60)

    return client
