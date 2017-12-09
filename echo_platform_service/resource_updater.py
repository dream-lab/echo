import threading
import time
import httplib
import json

# TODO put this in a properties file
mem_url = "/cat?href=/device/mem/"
cpu_url = "/cat?href=/device/cpu/"
meta_url = "/cat?href=/device/meta/"
ip_url = "/cat?href=/device/ip/"
# Till here


class resource_updater:

    registry_url = ""
    registry_port = None
    device_uuid = 0
    update_frequency = 60

    def __init__(self, registry_url, registry_port, device_uuid, update_frequency):
        self.registry_port = registry_port
        self.registry_url = registry_url
        self.device_uuid = device_uuid
        self.update_frequency = update_frequency

    def get_cpu_payload(self, id):
        cpu_data = dict()
        cpu_data['item-metadata'] = [
            {
                "val": "CPU Meta Data",
                "rel": "urn:X-hypercat:rels:hasDescription:en"
            },{
                "val": str(20),
                "rel": "CPUUtil"
            }
        ]
        cpu_data['href'] = '/device/cpu/' + id
        return json.dumps(cpu_data)

    def get_mem_payload(self, id):

        mem_data = dict()
        mem_data['href'] = '/device/mem/' + id

        mem_data['item-metadata'] = [
            {
                "val": "Memory Meta Data",
                "rel": "urn:X-hypercat:rels:hasDescription:en"
            },{
                "val": str(20),
                "rel": "MemUtil"
            }
        ]
        return json.dumps(mem_data)

    def update_registry(self, url, id, payload):
        conn = httplib.HTTPConnection(self.registry_url, self.registry_port)
        #header = {'Content-type': 'text/plain', 'Accept-Language': 'en-US,en;q=0.5'}
        conn.request('POST', url + id, payload )
        response = conn.getresponse()
        print self.registry_url
        print self.registry_port
        print url
        print id
        print payload
        print response.status

    def update_loop_trigger(self):
        while True:
            self.update_registry(mem_url, self.device_uuid, self.get_mem_payload(self.device_uuid))
            self.update_registry(cpu_url, self.device_uuid, self.get_cpu_payload(self.device_uuid))
            print "should be updated right?"
            #do the update
            time.sleep(self.update_frequency)

    def start_updater(self):
        thread = threading.Thread(target=self.update_loop_trigger)
        return thread.start()

    def register_device(self, address):
        meta_data = dict()
        meta_data['item-metadata'] = [
            {
                "val":"Device Meta Data",
                "rel": "urx:X-hypercat:rels:hasDescription:en"
            },{
                "val": self.device_uuid,
                "rel": "DeviceUUID"
            },{
                "val": "400",
                "rel": "Total CPU Available"
            },{
                "val": "1000",
                "rel": "Total Memory Available"
            },{
                "val": "1",
                "rel": "isAccelerated"
            }
        ]
        meta_data['href'] = '/device/meta/' + self.device_uuid
        ip_data = dict()
        ip_data['item-metadata'] = [
            {
                "val": "IP",
                "rel": "urn:X-hypercat:rels:hasDescription:en"
            },{
                "val": address,
                "rel": "IP"
            }
        ]
        ip_data['href'] = '/device/ip/' + self.device_uuid

        conn = httplib.HTTPConnection(self.registry_url, self.registry_port)
        #header = {'Content-type': 'text/plain', 'Accept-Language': 'en-US,en;q=0.5'}
        conn.request('POST', meta_url + self.device_uuid, json.dumps(meta_data))
        conn = httplib.HTTPConnection(self.registry_url, self.registry_port)
        conn.request('POST', ip_url + self.device_uuid, json.dumps(ip_data))




