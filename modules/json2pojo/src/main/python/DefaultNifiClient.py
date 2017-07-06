import json

import httplib
import time
import uuid
import importlib
#import NifiClient as NifiClient

NifiClient = importlib.import_module('in.dream_lab.echo.utils.NifiClient')
Connection = importlib.import_module('in.dream_lab.echo.utils.Connection')

class DefaultNifiClient(NifiClient):

    address = ''
    port = ''

    def __init__(self, address, port):
        self.address = address
        self.port = port

    def get_url(self):
        return "http://" + self.address + ":" + self.port + "/nifi/"

    def connect_to_nifi(self):
        conn = httplib.HTTPConnection(self.address, self.port)
        return conn

    def get_process_group(self):
        conn = self.connect_to_nifi()
        conn.request('GET', '/nifi-api/flow/process-groups/root')
        response = conn.getresponse()

        if response.status == 200:
            return json.loads(response.read().decode('utf-8'))


    def get_process_group_id(self,):
        return self.get_process_group()['processGroupFlow']['breadcrumb']['breadcrumb']['id']

    def get_all_processor_id(self, debug = False):
        conn = self.connect_to_nifi()
        conn.request('GET', '/nifi-api/flow/process-groups/root')
        response = conn.getresponse()

        if debug:
            print("GET on /flow/process-groups/root returned an HTTP status of " + response.status)

        if response.status == 200:
            payload = json.loads(response.read().decode("utf-8"))
            root_process_group_id = payload['processGroupFlow']['breadcrumb']['breadcrumb']['id']
            processors = payload['processGroupFlow']['flow']['processors']
            processor_ids = []
            for i in processors:
                processor_ids.append(i['id'])

            return processor_ids

    def get_all_connection_id(self, debug = False):
        conn = self.connect_to_nifi()
        conn.request('GET', '/nifi-api/flow/process-groups/root')
        response = conn.getresponse()

        if debug:
            print("GET on /flow/process-groups/root returned an HTTP status of " + response.status)

        if response.status == 200:
            payload = json.loads(response.read().decode("utf-8"))
            connections = payload['processGroupFlow']['flow']['connections']
            connection_ids = []
            for i in connections:
                connection_ids.append(i['id'])

            return connection_ids

    def get_connection(self, connection_id,):
        conn = self.connect_to_nifi()
        conn.request('GET', '/nifi-api/connections/' + connection_id)
        response = conn.getresponse()

        # TODO debug fork

        if response.status == 200:
            payload = json.loads(response.read().decode("utf-8"))
            return payload

    def get_processor(self, processor_id, debug = False):
        conn = self.connect_to_nifi()
        conn.request('GET', '/nifi-api/processors/'+processor_id)
        response = conn.getresponse()

        if debug:
            print("GET on /processor/"+processor_id+" returned an HTTP status of " + str(response.status))

        if response.status == 200:
            payload = json.loads(response.read().decode("utf-8"))
            return payload
        print(response.status)
        print(response.read())

    def get_connections(self):
        connections = self.get_all_connection_id()
        to_return = []
        for connection in connections:
            payload = self.get_connection(connection)
            object = Connection()
            object.setId(connection)
            object.setSource(payload['sourceId'])
            object.setDestination(payload['destinationId'])
            object.setSourceType(payload['sourceType'])
            object.setDestinationType(payload['destinationType'])
            object.setSourceGroupId(payload['sourceGroupId'])
            object.setDestinationGroupId(payload['destinationGroupId'])
            object.setDeleted(False)
            to_return.append(object)
        return to_return

    def get_processor_revision(self, processor_id):
        endpoint = '/nifi-api/processors/' + processor_id
        conn = self.connect_to_nifi()
        conn.request('GET', endpoint)
        # TODO handle response status.
        response = conn.getresponse()
        jData = json.loads(response.read().decode("utf-8"))
        return jData['revision']

    def stop_processor(self, processor_id):
        endpoint = '/nifi-api/processors/'+processor_id
        conn = self.connect_to_nifi()
        conn.request('PUT', endpoint,
                     json.dumps({"revision":self.get_processor_revision(processor_id), "component":{"id":processor_id, "state":"STOPPED"}}),
                     {"Content-Type": "application/json"})
        response = conn.getresponse()

        if response.status == 200:
            return True
        else:
            print(response.status)
            print(response.read())
            return False

    def start_processor(self, processor_id):
        endpoint = '/nifi-api/processors/'+processor_id
        conn = self.connect_to_nifi()
        revision = self.get_processor_revision(processor_id)
        conn.request('PUT', endpoint,
                     json.dumps({"revision":revision, "component":{"id":processor_id, "state":"RUNNING"}}), {"Content-Type":"application/json"})
        response = conn.getresponse()
        #print(revision)

        if response.status == 200:
            return True
        else:
            print(response.status)
            print(response.read())
            return False

    def is_started(self, processor_id):
        return self.get_processor(processor_id)['component']['state'] == 'RUNNING'

    def is_stopped(self, processor_id):
        return self.get_processor(processor_id)['component']['state'] == 'STOPPED'

    def delete_connection(self, connection_id):
        self.empty_connection(connection_id)
        return self.remove_connection(connection_id)

    def delete_processor(self, processor_id):
        if not self.is_stopped(processor_id):
            return False
        if len(self.get_adjacent_processors(processor_id)) > 0:
            print("hi")
            return False
        endpoint = '/nifi-api/processors/' + processor_id + '?version=' + str(self.get_processor_revision(processor_id)['version']) + '&clientId=' + str(uuid.uuid4())
        conn = self.connect_to_nifi()
        conn.request('DELETE', endpoint)
        response = conn.getresponse()
        if response.status == 200:
            print(response.status)
            return True

    def get_connections_for_processor(self, processor_id, debug = False):
        connections = self.get_all_connection_id()
        adjacent_connections = []
        for i in connections:
            connection = self.get_connection(i)
            if connection['sourceId'] == processor_id or connection['destinationId'] == processor_id:
                adjacent_connections.append(i)

        return adjacent_connections

    def get_adjacent_processors(self, processor_id, debug = False):
        adjacent_connections = self.get_connections_for_processor(processor_id)
        adjacent_processors = set()
        for i in adjacent_connections:
            connection = self.get_connection(i)
            adjacent_processors.add(connection['sourceId']
                                    if connection['destinationId']== processor_id
                                    else connection['destinationId'] )

        return adjacent_processors

    def get_downstream_processors(self, processor_id, debug = False):
        adjacent_connections = self.get_connections_for_processor(processor_id)
        downstream_processors = set()
        for i in adjacent_connections:
            connection = self.get_connection(i)
            if connection['sourceId'] == processor_id:
                downstream_processors.add(connection['destinationId'])
        return downstream_processors

    def get_downstream_connections(self, processor_id, debug = False):
        adjacent_connections = self.get_connections_for_processor(processor_id)
        downstream_connections = set()
        for i in adjacent_connections:
            connection = self.get_connection(i)
            if connection['sourceId'] == processor_id:
                downstream_connections.add(i)
        return downstream_connections

    def get_upstream_processors(self, processor_id, debug = False):
        adjacent_connections = self.get_connections_for_processor(processor_id)
        upstream_processors = set()
        for i in adjacent_connections:
            connection = self.get_connection(i)
            if connection['destinationId'] == processor_id:
                upstream_processors.add(connection['sourceId'])

        return upstream_processors

    def empty_connection(self, connection_id, debug = False):
        endpoint = '/nifi-api/flowfile-queues/' + connection_id + '/drop-requests'
        conn = self.connect_to_nifi()
        conn.request('POST', endpoint)
        response = conn.getresponse()

        if response.status == 200 or response.status == 202:
            payload = json.loads(response.read().decode("utf-8"))
            request_id = payload["dropRequest"]["id"]

            request_fulfilled = False
            while not request_fulfilled:
                conn = self.connect_to_nifi()
                conn.request('GET', endpoint + "/" + request_id)
                response = conn.getresponse()
                if not response.status == 200:
                    if debug:
                        print("mm")
                    return False
                payload = json.loads(response.read().decode("utf-8"))

                if payload['dropRequest']['state'] == "Completed successfully":
                    request_fulfilled = True
            conn = self.connect_to_nifi()
            conn.request('DELETE', endpoint + "/" + request_id)
            response = conn.getresponse()
            if not response.status == 200:
                if debug:
                    print("Hmm")
                return False
            return True

        if debug:
            print(response.status)
            print(response.read().decode("utf-8"))
        return False

    def get_connection(self, connection_id, debug = False):
        endpoint = '/nifi-api/connections/' + connection_id
        conn = self.connect_to_nifi()
        conn.request('GET', endpoint)
        response = conn.getresponse()

        if response.status == 200:
            payload = json.loads(response.read().decode('utf-8'))
            return payload

    def get_connection_version(self, connection_id, debug = False):
        version = self.get_connection(connection_id)['revision']['version']
        return version

    def remove_connection(self, connection_id, debug = False):
        endpoint = '/nifi-api/connections/' + connection_id + '?version=' + str(self.get_connection_version(connection_id)) + '&clientId=' + str(uuid.uuid4())
        conn = self.connect_to_nifi()
        conn.request('DELETE', endpoint)
        response = conn.getresponse()

        if response.status == 200:
            # payload = json.loads(response.read().decode('utf-8'))
            return True
        return False

    def get_input_ports(self, debug = False):
        conn = self.connect_to_nifi()
        conn.request('GET', '/nifi-api/flow/process-groups/root')
        response = conn.getresponse()
        if response.status == 200:
            payload = json.loads(response.read().decode('utf-8'))
            input_ports = payload['processGroupFlow']['flow']['inputPorts']
            input_port_ids = []
            for port in input_ports:
                input_port_ids.append(port['id'])
            return input_port_ids

    def get_input_port(self, port_id, debug = False):
        conn = self.connect_to_nifi()
        conn.request('GET', '/nifi-api/input-ports/' + port_id)
        response = conn.getresponse()
        if response.status == 200:
            payload = json.loads(response.read().decode('utf-8'))
            return payload

    def create_new_input_port(self, name, debug = False):
        process_group_id = self.get_process_group_id()
        endpoint = '/nifi-api/process-groups/' + process_group_id + '/input-ports'
        conn = self.connect_to_nifi()
        conn.request('POST', endpoint,
                     json.dumps(
                         {"component": {"name": name},
                         "revision": {"clientId": str(uuid.uuid4()),
                                       "version": 0}}
                     ), {"Content-Type": "application/json"})
        response = conn.getresponse()
        if response.status == 201:
            #print(response.read().decode('utf-8'))
            return json.loads(response.read().decode('utf-8'))['id']
        print(response.status)
        print(response.read().decode('utf-8'))
        return None

    def delete_input_port(self, id):
        conn = self.connect_to_nifi()
        payload = self.get_input_port(id)
        conn.request('DELETE',
                     '/nifi-api/input-ports/' + id + '?version=' + str(payload['revision']['version']) + "&clientId="+str(uuid.uuid4()))
        response = conn.getresponse()
        if response.status == 200:
            return True
        print(response.status)
        print(response.read().decode('utf-8'))
        return False

    def create_connection(self, from_id, to_id, relationship_name):
        # check what kind of objects the IDs are.
        from_type = None
        to_type = None
        conn = self.connect_to_nifi()
        from_processor = None
        conn.request('GET', '/nifi-api/flow/process-groups/root')
        response = conn.getresponse()
        if response.status == 200:
            payload = json.loads(response.read().decode('utf-8'))
            input_ports = payload['processGroupFlow']['flow']['inputPorts']
            output_ports = payload['processGroupFlow']['flow']['outputPorts']
            rpgs = payload['processGroupFlow']['flow']['remoteProcessGroups']
            processors = payload['processGroupFlow']['flow']['processors']
            for i in input_ports:
                if from_id == i['id']:
                    from_type = 'INPUT_PORT'
                if to_id == i['id']:
                    to_type = 'INPUT_PORT'
            for i in output_ports:
                if from_id == i['id']:
                    from_type = 'OUTPUT_PORT'
                if to_id == i['id']:
                    to_type = 'OUTPUT_PORT'
            for i in rpgs:
                if from_id == i['id']:
                    from_type = 'REMOTE_OUTPUT_PORT'
                if to_id == i['id']:
                    to_type = 'REMOTE_INPUT_PORT'
            for i in processors:
                if from_id == i['id']:
                    from_type = 'PROCESSOR'
                    from_processor = i
                if to_id == i['id']:
                    to_type = 'PROCESSOR'
        # check if the given relationship exists for the given object
        if from_type == 'PROCESSOR':
            flag = False
            for i in from_processor['component']['relationships']:
                if i['name'] == relationship_name and not i['autoTerminate']:
                    flag = True
            if not flag:
                None
                # TODO error raised

        # figure out the client ID and the version numbers for the same
        put_body = json.dumps(
            {
                "component": {
                    "name": "",
                    "source": {
                        "id": from_id,
                        "groupId": self.get_process_group_id(),
                        "type": from_type
                    },
                    "destination": {
                        "id": to_id,
                        "groupId": self.get_process_group_id(),
                        "type": to_type
                    },
                    "selectedRelationships": [relationship_name],
                    "flowFileExpiration": "0 sec",
                    "backPressureDataSizeThreshold": "1 GB",
                    "backPressureObjectThreshold": "10000",
                    "bends": [],
                    "prioritizers": []
                },
                "revision": {"clientId": str(uuid.uuid4()), "version": 0}
            }
        )

        conn = self.connect_to_nifi()
        conn.request("POST", '/nifi-api/process-groups/' + self.get_process_group_id() + '/connections', put_body,
                     {'Content-Type':'application/json'})
        response = conn.getresponse()
        if response.status == 200 or response.status == 201:
            return True
        print(response.status)
        print(response.read())
        return False

    def start_input_port(self, port_id, debug = False):
        endpoint = '/nifi-api/input-ports/' + port_id
        conn = self.connect_to_nifi()
        put_body = json.dumps(
            {
                "component" : {"id": port_id, "state": "RUNNING"},
                "revision": {"clientId": str(uuid.uuid4()), "version": self.get_input_port(port_id)['revision']['version']}
            }
        )
        conn.request('PUT', endpoint, put_body, {'Content-Type':'application/json'})
        response = conn.getresponse()
        if response.status == 200:
            return True
        return False

    def create_remote_process_group(self, target_uri):
        pg_id = self.get_process_group_id()
        endpoint = '/nifi-api/process-groups/' + pg_id + '/remote-process-groups'
        post_body = json.dumps(
            {
                "component": {
                    "targetUri": target_uri,
                    "position": {
                        "x": 61.0,
                        "y": -839.0,
                    },
                    "communicationsTimeout": "30 sec",
                    "yieldDuration": "10 sec",
                    "transportProtocol": "RAW",
                    "proxyHost": "",
                    "proxyPort": "",
                    "proxyUser": "",
                    "proxyPassword": ""
                }, "revision": {
                    "clientId": str(uuid.uuid4()),
                    "version": 0
                }
            }
        )
        conn = self.connect_to_nifi()
        conn.request('POST', endpoint, post_body, {'Content-Type':'application/json'})
        response = conn.getresponse()
        if response.status == 201:
            return json.loads(response.read().decode('utf-8'))['id']
        print(response.status)
        print(response.read())

    def delete_remote_process_group(self, id):
        conn = self.connect_to_nifi()
        version = self.get_remote_process_group(id)['revision']['version']
        clientId = uuid.uuid4()
        conn.request('DELETE', '/nifi-api/remote-process-groups/' + id + '?version=' + str(version) + '&clientId=' + str(clientId))
        response = conn.getresponse()
        if response.status == 200:
            return True
        return False

    def get_remote_process_group(self, id):
        conn = self.connect_to_nifi()
        conn.request('GET', '/nifi-api/remote-process-groups/' + id)
        response = conn.getresponse()
        if response.status == 200:
            return json.loads(response.read().decode('utf-8'))

    def rpg_available_ports(self, rpgid, input = True):
        ports = []
        jsonobject = 'inputPorts' if input else 'outputPorts'
        for port in self.get_remote_process_group(rpgid)['component']['contents'][jsonobject]:
            ports.append(port['id'])
        return ports

    def get_all_rpgs(self):
        proc_json = self.get_process_group()
        rpgs = []
        for rpg in proc_json['processGroupFlow']['flow']['remoteProcessGroups']:
            rpgs.append(rpg['id'])
        return rpgs

    def connect_remote_output_port(self, rpg_id, port_id, proc_id):
        return self.connect_remote_input_port(rpg_id, port_id, proc_id, None, False)

    def connect_remote_input_port(self, rpg_id, port_id, proc_id, relationship_name, input = True):
        # figure out the client ID and the version numbers for the same
        from_id = None
        to_id = None
        from_group = None
        to_group = None
        if input:
            from_id = proc_id
            from_group = self.get_process_group_id()
            to_id = port_id
            to_group = rpg_id
            to_type = "REMOTE_INPUT_PORT"
            from_type = "PROCESSOR"
        else:
            from_id = port_id
            from_group = rpg_id
            to_id = proc_id
            to_group = self.get_process_group_id()
            to_type = "PROCESSOR"
            from_type = "REMOTE_OUTPUT_PORT"

        while port_id not in self.rpg_available_ports(rpg_id, input):
            time.sleep(1)

        put_body = json.dumps(
            {
                "component": {
                    "name": "",
                    "source": {
                        "id": from_id,
                        "groupId": from_group,
                        "type": from_type
                    },
                    "destination": {
                        "id": to_id,
                        "groupId": to_group,
                        "type": to_type
                    },
                    "selectedRelationships": [relationship_name if relationship_name is not None else ''],
                    "flowFileExpiration": "0 sec",
                    "backPressureDataSizeThreshold": "1 GB",
                    "backPressureObjectThreshold": "10000",
                    "bends": [],
                    "prioritizers": []
                },
                "revision": {"clientId": str(uuid.uuid4()), "version": 0},
                "sourceGroupId": from_group,
                "sourceId": from_id,
                "sourceType": from_type,
                "destinationGroupId": to_group,
                "destinationId": to_id,
                "destinationType": to_type
            }
        )
        print(put_body)

        conn = self.connect_to_nifi()
        conn.request("POST", '/nifi-api/process-groups/' + self.get_process_group_id() + '/connections', put_body,
                     {'Content-Type':'application/json'})
        response = conn.getresponse()
        if response.status == 200 or response.status == 201:
            return True
        print(response.status)
        print(response.read())
        return False

    def enable_rpg_transmission(self, rpg_id):
        put_body = json.dumps({
            "component": {
                "id": rpg_id,
                "transmitting": True
            }, "revision": {
                "clientId": str(uuid.uuid4()),
                "version": self.get_remote_process_group(rpg_id)['revision']['version']
            }
        })
        conn = self.connect_to_nifi()
        conn.request('PUT', '/nifi-api/remote-process-groups/' + rpg_id, put_body, {'Content-Type':'application/json'})
        response = conn.getresponse()
        if response.status == 200:
            return True
        print(response.status)
        print(response.read())
        return False

    def disable_rpg_transmission(self, rpg_id):
        put_body = json.dumps({
            "component": {
                "id": rpg_id,
                "transmitting": False
            }, "revision": {
                "clientId": str(uuid.uuid4()),
                "version": self.get_remote_process_group(rpg_id)['revision']['version']
            }
        })
        conn = self.connect_to_nifi()
        conn.request('PUT', '/nifi-api/remote-process-groups/' + rpg_id, put_body, {'Content-Type':'application/json'})
        response = conn.getresponse()
        if response.status == 200:
            return True
        print(response.status)
        print(response.read())
        return False

    def get_output_port(self, port_id):
        return self.get_input_port(port_id, False)

    def enable_output_port(self, port_id):
        return self.enable_input_port(port_id, False)

    def disable_output_port(self, port_id):
        return self.enable_input_port(port_id, False, True)

    def get_input_port(self, port_id, input_port = True):
        endpoint = None
        if input_port:
            endpoint = '/nifi-api/input-ports/' + port_id
        else:
            endpoint = '/nifi-api/output-ports/' + port_id
        conn = self.connect_to_nifi()
        conn.request('GET', endpoint)
        response = conn.getresponse()
        if response.status == 200:
            return json.loads(response.read().decode('utf-8'))
        print(response.status)
        print(response.read())

    def enable_input_port(self, port_id, input_port = True, disable = False):
        if input_port:
            endpoint = '/nifi-api/input-ports/' + port_id
        else:
            endpoint = '/nifi-api/output-ports/' + port_id
        put_body = json.dumps({
            "component" : {
                "id": port_id,
                "state": "RUNNING" if not disable else "STOPPED"
            }, "revision": {
                "clientId": str(uuid.uuid4()),
                "version": self.get_input_port(port_id, input_port)['revision']['version']
            }
        })
        conn = self.connect_to_nifi()
        conn.request('PUT', endpoint, put_body, {'Content-Type':'application/json'})
        response = conn.getresponse()
        if response.status == 200:
            return True
        print(response.status)
        print(response.read())
        return False

    def disable_input_port(self, port_id):
        return self.enable_input_port(port_id, True, True)

    def create_processor(self, type, name):
        conn = self.connect_to_nifi()
        post_body = json.dumps({
            'component': {
                'type': type,
                'name': name,
                'position': {
                    'x': -1279,
                    'y': 21
                }
            }, 'revision' :{
                'clientId': str(uuid.uuid4()),
                'version': 0
            }
        })
        conn.request("POST", '/nifi-api/process-groups/' + self.get_process_group_id() + '/processors',
                     post_body, {"Content-Type":"application/json"})
        response = conn.getresponse()
        if response.status == 200 or response.status == 201:
            return json.loads(response.read().decode('utf-8'))['id']
        print(response.status)
        print(response.read())

    def set_processor_properties_and_relationships(self, proc_id, properties, relationships, config):
        component_json = {}
        component_json['relationships'] = json.loads(relationships)
        #component_json['relationships'] = relationships
        component_json['config'] = json.loads(config)
        component_json['config']['properties'] = json.loads(properties)
        #component_json['config']['properties'] = properties
        return self.copy_processor_properties(proc_id, component_json)

    def copy_processor_properties(self, proc_id, component_json, retry=0):
        conn = self.connect_to_nifi()
        component_json['id'] = proc_id
        component_json['parentGroupId'] = self.get_process_group_id()
        revision_json = self.get_processor_revision(proc_id)
        autoTerminatedRelationships = []
        for i in component_json['relationships']:
            if i.get('autoTerminate'):
            #if i['autoTerminate']:
                autoTerminatedRelationships.append(i)
        component_json['config']['autoTerminatedRelationships'] = autoTerminatedRelationships
        post_body = '{ "component": ' + json.dumps(component_json) + ', "revision": ' + json.dumps(revision_json) + '}'
        conn.request("PUT", "/nifi-api/processors/" + proc_id, post_body, {"Content-Type":"application/json"})
        response = conn.getresponse()
        if response.status == 200:
            return True
        if response.status == 409 and retry < 3:
            time.sleep(retry+1)
            return self.copy_processor_properties(proc_id, component_json, retry + 1)
        print(response.status)
        print(response.read())
        return False

    def get_all_input_ports(self):
        proc_json = self.get_process_group()
        ports = []
        for port in proc_json['processGroupFlow']['flow']['inputPorts']:
            ports.append(port['id'])
        return ports

