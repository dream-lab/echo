import threading
import time
import httplib
import json

# TODO put this in a properties file
registry_url = "13.71.125.147"
registry_port = 8080
device_uuid = "1"
mem_url = "?href=/device/mem/"
cpu_url = "?href=/device/cpu/"
# Till here

def create_payload():
    data = dict()
    data['item-metadata'] = [
        {"rel": "urx:X-hypercat:rels:hasDescription:en"},
        {"rel": "CPUUtil"}
    ]
    return data

def get_mem_payload(id):
    cpu_data = create_payload()
    cpu_data['item-metadata'][0]['val'] = 'CPU Meta Data'
    cpu_data['item-metadata'][1]['val'] = str(20)
    cpu_data['href'] = '/device/cpu/' + id
    return json.dumps(cpu_data)


def get_cpu_payload(id):
    mem_data = create_payload()
    mem_data['item-metadata'][0]['val'] = 'Memory Meta Data'
    mem_data['item-metadata'][1]['val'] = str(20)
    mem_data['href'] = '/device/mem/' + id
    return json.dumps(mem_data)


def update_registry(url, id, payload):
    conn = httplib.HTTPConnection(registry_url, registry_port)
    header = {'Content-type': 'text/plain', 'Accept-Language': 'en-US,en;q=0.5'}
    conn.request('POST', url + id, payload, header)


def update_loop_trigger():
    while True:
        update_registry(mem_url, device_uuid, get_mem_payload(device_uuid))
        update_registry(cpu_url, device_uuid, get_cpu_payload(device_uuid))
        #do the update
        time.sleep(60)


def start_updater():
    thread = threading.Thread(target=update_loop_trigger)
    return thread.start()

