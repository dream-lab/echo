import threading
import time
import httplib
import json
import os
import datetime

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
    usage_file = '/app/resource_usage.log'

    def __init__(self, registry_url, registry_port, device_uuid, update_frequency, usage_file):
        self.registry_port = registry_port
        self.registry_url = registry_url
        self.device_uuid = device_uuid
        self.update_frequency = update_frequency
        self.usage_file = usage_file

    def get_cpu_usage(self):
        return str(round(float(os.popen('''grep 'cpu ' /proc/stat | awk '{usage=($2+$4)*100/($2+$4+$5)} END {print usage }' ''').readline()),2))

    def get_mem_usage(self):
        tot_m, used_m, free_m = map(int, os.popen('free -t -m').readlines()[-1].split()[1:])
        return str(used_m / tot_m * 100)

    def get_cpu_payload(self, id):
        cpu_data = dict()
        cpu_data['item-metadata'] = [
            {
                "val": "CPU Meta Data",
                "rel": "urn:X-hypercat:rels:hasDescription:en"
            },{
                "val": self.get_cpu_usage(),
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
                "val": self.get_mem_usage(),
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
            with open(self.usage_file, 'a') as myfile:
                for i in range(4):
                    timestamp = str(datetime.datetime.now()).split('.')[0]
                    myfile.write('{} CPU usage = {}    MEM usage = {}\n'.format(timestamp, self.get_cpu_usage(), self.get_mem_usage()))
                    time.sleep(self.update_frequency/4)
                    myfile.flush()

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

