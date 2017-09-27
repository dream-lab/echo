import mqttclient
import resource_updater

device_uuid = '1'


client = mqttclient.get_client("10.24.24.222", device_uuid)
# Self registering could code possibly go here.
# In fact NiFi startup could also go here.
# This should be run at startup inside the container.
updater_thread = resource_updater.start_updater()
client.subscribe(device_uuid)
client.loop_forever()

