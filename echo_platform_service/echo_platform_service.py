#!/usr/bin/python2

import mqttclient
import resource_updater
import sys
import atexit


# TODO Here goes the logic to handle the assasination of the updater thread
def exit_function():
    None


device_uuid = sys.argv[1]
registry_url = sys.argv[2]
registry_port = '8080'
mqtt_ip = sys.argv[3]
kafka_ip = sys.argv[4]
update_frequency = int(sys.argv[5])
ip_address = sys.argv[6]
usage_file = sys.argv[7]

updater = resource_updater.resource_updater(registry_url, registry_port, device_uuid, update_frequency, usage_file)
updater.register_device(ip_address)
client = mqttclient.get_client(mqtt_ip, device_uuid, kafka_ip)
# Self registering could code possibly go here.
# In fact NiFi startup could also go here.
# This should be run at startup inside the container.
updater_thread = updater.start_updater()
client.subscribe(device_uuid,2)
client.loop_forever()
