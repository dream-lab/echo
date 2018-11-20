import paho.mqtt.client as mqtt
import paho.mqtt.publish as mqttpublish
import NifiClient
import json
import time
import uuid
import logging

mqttHost = '13.71.125.147'
kafkabroker = '13.71.125.147'

logging.basicConfig()
logger = logging.getLogger('mqttclient')

def on_connect(client, userdata, flags, rc):
    print "Connected to broker"

def create_processor(params):
    client = NifiClient.NifiClient('127.0.0.1', 8080)
    relationships = params['relationships']
    configs = params['configs']
    properties = params['properties']
    name = params['name']
    class_ = params['class']
    id = None

    retry = 0
    while retry < 3:
        try:
            id = client.create_processor(class_, name)
        except NifiClient.NifiClient.RetryError as err:
            logger.warning('create_output_port: %s', 'NiFi in bad state, retrying')
            if retry >= 3:
                raise err
        except NifiClient.NifiClient.FatalError as err:
            logger.error('%s: %s, \n status : %s \n message: %s', 'NiFi returned invalid message',
                          'create_processor', err.code, err.message)
            if retry >= 3:
                raise err
            raise err
        finally:
            retry +=1
            time.sleep(3)
            if id is not None:
                break

    retry = 0
    while retry < 3:
        try:
            client.set_processor_properties_and_relationships(id, properties, relationships, configs)
        except NifiClient.NifiClient.RetryError as err:
            logger.warning('create_output_port: %s', 'NiFi in bad state, retrying')
            if retry >= 3:
                raise err
        except NifiClient.NifiClient.FatalError as err:
            logger.error('%s: %s, \n status : %s \n message: %s', 'NiFi returned invalid message',
                          'set_processor_properties_and_relationships', err.code, err.message)
            if retry >= 3:
                raise err
            raise err
        finally:
            retry +=1
            time.sleep(3)
            break

    return id


def create_input_port(params):
    client = NifiClient.NifiClient('127.0.0.1', 8080)
    name = params['name']
    id = None
    retry = 0
    while retry < 3:
        try:
            id = client.create_new_input_port(name)
        except NifiClient.NifiClient.RetryError as err:
            logger.warning('create_output_port: %s', 'NiFi in bad state, retrying')
            if retry >= 3:
                raise err
        except NifiClient.NifiClient.FatalError as err:
            logger.error('%s: %s, \n status : %s \n message: %s', 'NiFi returned invalid message',
                          'create_output_port', err.code, err.message)
            if retry >= 3:
                raise err
            raise err
        finally:
            retry +=1
            time.sleep(3)
            if id is not None:
                break
    return id


def enable_input_port(params):
    client = NifiClient.NifiClient('127.0.0.1', 8080)
    id = params['port_id']
    retry = 0
    while retry < 3:
        try:
            toReturn = client.enable_input_port(id)
        except NifiClient.NifiClient.RetryError as err:
            logger.warning('create_output_port: %s', 'NiFi in bad state, retrying')
            if retry >= 3:
                raise err
        except NifiClient.NifiClient.FatalError as err:
            logger.error('%s: %s, \n status : %s \n message: %s', 'NiFi returned invalid message',
                          'create_output_port', err.code, err.message)
            if retry >= 3:
                raise err
            raise err
        finally:
            retry +=1
            if toReturn is True:
                break
            time.sleep(3)
    return toReturn


def create_output_port(params):
    client = NifiClient.NifiClient('127.0.0.1', 8080)
    name = params['name']
    id = None
    retry = 0
    while retry < 3:
        try:
            id = client.create_new_output_port(name)
        except NifiClient.NifiClient.RetryError as err:
            logger.warning('create_output_port: %s', 'NiFi in bad state, retrying')
            if retry >= 3:
                raise err
        except NifiClient.NifiClient.FatalError as err:
            logger.error('%s: %s, \n status : %s \n message: %s', 'NiFi returned invalid message',
                          'create_output_port', err.code, err.message)
            if retry >= 3:
                raise err
            raise err
        finally:
            retry +=1
            time.sleep(3)
            if id is not None:
                break

    retry = 0
    while retry < 3:
        try:
            client.enable_output_port(id)
        except NifiClient.NifiClient.RetryError as err:
            logger.warning('enable_output_port: %s', 'NiFi in bad state, retrying')
            if retry >= 3:
                raise err
        except NifiClient.NifiClient.FatalError as err:
            logger.error('%s: %s, \n status : %s \n message: %s', 'NiFi returned invalid message',
                          'enable_output_port', err.code, err.message)
            if retry >= 3:
                raise err
        finally:
            retry +=1
            if id is not None:
                break
            time.sleep(3)

    return id


def create_rpg(params):
    client = NifiClient.NifiClient('127.0.0.1', 8080)
    url = params['url']
    id = None
    retry = 0
    while retry < 3:
        try:
            id = client.create_remote_process_group(url)
        except NifiClient.NifiClient.RetryError as err:
            logger.warning('enable_output_port: %s', 'NiFi in bad state, retrying')
            if retry >= 3:
                raise err
        except NifiClient.NifiClient.FatalError as err:
            logger.error('%s: %s, \n status : %s \n message: %s', 'NiFi returned invalid message',
                          'enable_output_port', err.code, err.message)
            if retry >= 3:
                raise err
        finally:
            retry +=1
            if id is not None:
                break
            time.sleep(3)
    return id


def create_connection(params):
    client = NifiClient.NifiClient('127.0.0.1', 8080)
    from_id = params['from_id']
    to_id = params['to_id']
    relationship_name = params['relationship_name']
    id = None
    retry = 0
    while retry < 3:
        try:
            id = client.create_connection(from_id, to_id, relationship_name)
        except NifiClient.NifiClient.RetryError as err:
            logger.warning('enable_output_port: %s', 'NiFi in bad state, retrying')
            if retry >= 3:
                raise err
        except NifiClient.NifiClient.FatalError as err:
            logger.error('%s: %s, \n status : %s \n message: %s', 'NiFi returned invalid message',
                          'enable_output_port', err.code, err.message)
            if retry >= 3:
                raise err
        finally:
            retry +=1
            if id is not None:
                break
            time.sleep(3)
    return id

def connect_remote_input_port(params):
    client = NifiClient.NifiClient('127.0.0.1', 8080)
    rpg_id = params['rpg']
    port_id = params['port_id']
    proc_id = params['proc_id']
    relationship = params['relationship']
    id = None
    retry = 0
    while retry < 3:
        try:
            id = client.connect_remote_input_port(rpg_id, port_id, proc_id, relationship)
        except NifiClient.NifiClient.RetryError as err:
            logger.warning('enable_output_port: %s', 'NiFi in bad state, retrying')
            if retry >= 3:
                raise err
        except NifiClient.NifiClient.FatalError as err:
            logger.error('%s: %s, \n status : %s \n message: %s', 'NiFi returned invalid message',
                          'enable_output_port', err.code, err.message)
            if retry >= 3:
                raise err
        finally:
            retry +=1
            if id is not None:
                break
            time.sleep(3)

    retry = 0
    while retry < 3:
        try:
            client.enable_rpg_transmission(rpg_id, port_id, True)
            break
        except NifiClient.NifiClient.RetryError as err:
            logger.warning('enable_output_port: %s', 'NiFi in bad state, retrying')
            if retry >= 3:
                raise err
        except NifiClient.NifiClient.FatalError as err:
            logger.error('%s: %s, \n status : %s \n message: %s', 'NiFi returned invalid message',
                          'enable_output_port', err.code, err.message)
            if retry >= 3:
                raise err
        finally:
            retry +=1
            time.sleep(3)
    return id


def connect_remote_output_port(params):
    client = NifiClient.NifiClient('127.0.0.1', 8080)
    rpg_id = params['rpg']
    port_id = params['port_id']
    proc_id = params['proc_id']
    relationship = params['relationship']
    id = None
    retry = 0
    while retry < 3:
        try:
            id = client.connect_remote_output_port(rpg_id, port_id, proc_id, relationship)
        except NifiClient.NifiClient.RetryError as err:
            logger.warning('enable_output_port: %s', 'NiFi in bad state, retrying')
            if retry >= 3:
                raise err
        except NifiClient.NifiClient.FatalError as err:
            logger.error('%s: %s, \n status : %s \n message: %s', 'NiFi returned invalid message',
                          'enable_output_port', err.code, err.message)
            if retry >= 3:
                raise err
        finally:
            retry +=1
            if id is not None:
                break
            time.sleep(3)

    retry = 0
    while retry < 3:
        try:
            client.enable_rpg_transmission(rpg_id, port_id, False)
            break
        except NifiClient.NifiClient.RetryError as err:
            logger.warning('enable_output_port: %s', 'NiFi in bad state, retrying')
            if retry >= 3:
                raise err
        except NifiClient.NifiClient.FatalError as err:
            logger.error('%s: %s, \n status : %s \n message: %s', 'NiFi returned invalid message',
                          'enable_output_port', err.code, err.message)
            if retry >= 3:
                raise err
        finally:
            retry +=1
            time.sleep(3)
    return id


def start_processor(params):
    client = NifiClient.NifiClient('127.0.0.1', 8080)
    proc_id = params['proc_id']
    id = None
    retry = 0
    while retry < 3:
        try:
            id = client.start_processor(proc_id)
            break
        except NifiClient.NifiClient.RetryError as err:
            logger.warning('enable_output_port: %s', 'NiFi in bad state, retrying')
            if retry >= 3:
                raise err
        except NifiClient.NifiClient.FatalError as err:
            logger.error('%s: %s, \n status : %s \n message: %s', 'NiFi returned invalid message',
                          'enable_output_port', err.code, err.message)
            if retry >= 3:
                raise err
        finally:
            retry +=1
            if id is not None:
                break
            time.sleep(3)
    return id


def create_kafka_consumer(params):
    client = NifiClient.NifiClient('127.0.0.1', 8080)
    topic = params['topic']
    processor_class = 'in.dream_lab.echo.processors.KafkaFlowFilesConsumer'
    processor_name = 'kafka_consumer_' + topic
    id = None
    retry = 0
    while retry < 3:
        try:
            id = client.create_processor(processor_class, processor_name)
        except NifiClient.NifiClient.RetryError as err:
            logger.warning('enable_output_port: %s', 'NiFi in bad state, retrying')
            if retry >= 3:
                raise err
        except NifiClient.NifiClient.FatalError as err:
            logger.error('%s: %s, \n status : %s \n message: %s', 'NiFi returned invalid message',
                          'enable_output_port', err.code, err.message)
            if retry >= 3:
                raise err
        finally:
            retry +=1
            if id is not None:
                break
            time.sleep(3)

    properties = dict()
    properties['topic'] = topic
    properties['group_name'] = str(uuid.uuid4())
    properties['brokerip'] = kafkabroker
    relationships = []
    relationships.append({'name': 'success', 'autoTerminate': False})
    retry = 0
    while retry < 3:
        try:
            client.set_processor_properties_and_relationships(id, json.dumps(properties),
                                                              json.dumps(relationships), json.dumps({}))
            break
        except NifiClient.NifiClient.RetryError as err:
            logger.warning('enable_output_port: %s', 'NiFi in bad state, retrying')
            if retry >= 3:
                raise err
        except NifiClient.NifiClient.FatalError as err:
            logger.error('%s: %s, \n status : %s \n message: %s', 'NiFi returned invalid message',
                          'enable_output_port', err.code, err.message)
            if retry >= 3:
                raise err
        finally:
            retry +=1
            time.sleep(3)
    return id


def create_kafka_producer(params):
    client = NifiClient.NifiClient('127.0.0.1', 8080)
    topic = params['topic']
    processor_class = 'in.dream_lab.echo.processors.KafkaFlowFilesProducer'
    processor_name = 'kafka_producer_' + topic
    id = None
    retry = 0
    while retry < 3:
        try:
            id = client.create_processor(processor_class, processor_name)
        except NifiClient.NifiClient.RetryError as err:
            logger.warning('enable_output_port: %s', 'NiFi in bad state, retrying')
            if retry >= 3:
                raise err
        except NifiClient.NifiClient.FatalError as err:
            logger.error('%s: %s, \n status : %s \n message: %s', 'NiFi returned invalid message',
                          'enable_output_port', err.code, err.message)
            if retry >= 3:
                raise err
        finally:
            retry +=1
            if id is not None:
                break
            time.sleep(3)
    properties = dict()
    properties['topic'] = topic
    properties['brokerip'] = kafkabroker
    relationships = []
    relationships.append({'name': 'success', 'autoTerminate': True})
    retry = 0
    while retry < 3:
        try:
            client.set_processor_properties_and_relationships(id, json.dumps(properties),
                                                              json.dumps(relationships), json.dumps({}))
            break
        except NifiClient.NifiClient.RetryError as err:
            logger.warning('enable_output_port: %s', 'NiFi in bad state, retrying')
            if retry >= 3:
                raise err
        except NifiClient.NifiClient.FatalError as err:
            logger.error('%s: %s, \n status : %s \n message: %s', 'NiFi returned invalid message',
                          'enable_output_port', err.code, err.message)
            if retry >= 3:
                raise err
        finally:
            retry +=1
            time.sleep(3)
    return id


def enable_kafka_port(params):
    client = NifiClient.NifiClient('127.0.0.1', 8080)
    port_id = params['port_id']
    id = None
    retry = 0
    while retry < 3:
        try:
            id = client.start_processor(port_id)
            break
        except NifiClient.NifiClient.RetryError as err:
            logger.warning('enable_output_port: %s', 'NiFi in bad state, retrying')
            if retry >= 3:
                raise err
        except NifiClient.NifiClient.FatalError as err:
            logger.error('%s: %s, \n status : %s \n message: %s', 'NiFi returned invalid message',
                          'enable_output_port', err.code, err.message)
            if retry >= 3:
                raise err
        finally:
            retry +=1
            if id is None:
                break
            time.sleep(3)
    return id


def stop_processor(params):
    client = NifiClient.NifiClient('127.0.0.1', 8080)
    processor_id = params['proc_id']
    retry = 0
    while retry < 3:
        try:
            client.stop_processor(processor_id)
            break
        except NifiClient.NifiClient.RetryError as err:
            logger.warning('enable_output_port: %s', 'NiFi in bad state, retrying')
            if retry >= 3:
                raise err
        except NifiClient.NifiClient.FatalError as err:
            logger.error('%s: %s, \n status : %s \n message: %s', 'NiFi returned invalid message',
                          'enable_output_port', err.code, err.message)
            if retry >= 3:
                raise err
        finally:
            retry +=1
            time.sleep(3)

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
        response = None
        try:
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
                type = 'RPG'
            elif methodName == 'disable_kafka_port':
                id = disable_kafka_port(params)
                type = 'KAFKA_PORT' 
            elif methodName == 'purge_connection':
                id = purge_connection(params)
                type = 'CONNECTION'
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
            response = dict([('id', id), ('type', type)])
        except NifiClient.NifiClient.RetryError as err:
            response = dict([('id', id),('type', 'RetryError'),('expr', err.expression),('message',err.message)])
        except NifiClient.NifiClient.FatalError as err:
            response = dict([('id', id),('type', 'RetryError'),('expr', err.expression),('code',err.code),('message',err.message)])
        finally:
            datagram['responseSet'][str(i)] = response
            print methodName
            i += 1
    publish_payload = json.dumps(datagram)
    mqttpublish.single(payload['ackTopic'], publish_payload, 2, hostname = mqttHost, port = 1883,
                       client_id=payload['resourceId'])
    #print methods[0]['methodName']
    #print(msg.topic + " " + str(msg.payload))


def get_client(broker, topic, kafka_ip):
    #client = mqtt.Client()
    client = mqtt.Client(client_id='Client-' + topic)
    client.on_connect = on_connect
    client.on_message = on_message
    global mqttHost
    mqttHost = broker
    global kafkabroker
    kafkabroker = kafka_ip + ":9092"


    client.connect(broker, 1883, 60)

    return client
