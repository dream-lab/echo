import mqttclient
import resource_updater

device_uuid = '17'
registry_url = '13.71.125.147'
registry_port = '8080'
mqtt_client = "13.71.125.147"
ip_address = "10.24.24.222"


updater = resource_updater.resource_updater(registry_url, registry_port, device_uuid)
updater.register_device(ip_address)
client = mqttclient.get_client("13.71.125.147", device_uuid)
# Self registering could code possibly go here.
# In fact NiFi startup could also go here.
# This should be run at startup inside the container.
updater_thread = updater.start_updater()
client.subscribe(device_uuid)
client.loop_forever()
