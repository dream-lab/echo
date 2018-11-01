#!/usr/bin/python

import sys
import socket
import subprocess
import time
import fileinput

# The program needs to be executed as
# bootstrap.py <device_uuid> <registry_url> <mqtt_client> <resource_update_frequency<>

device_uuid = sys.argv[1]
registry_url = sys.argv[2]
mqtt_client = sys.argv[3]
kafka_ip = sys.argv[4]
resource_update_frequency = sys.argv[5]
usage_file = sys.argv[6]

# to retrieve the local IP address
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.connect(("8.8.8.8", 80))
local_ip = s.getsockname()[0]
s.close()

text = ''

# change the IP address in nifi.properites
with open('/app/nifi-1.2.0/conf/nifi.properties', 'r') as in_file:
    text = in_file.read()

with open('/app/nifi-1.2.0/conf/nifi.properties', 'w') as out_file:
    text = text.replace('nifi.remote.input.host=', 'nifi.remote.input.host='+local_ip)
    out_file.write(text)
print "Changed nifi.properties file. Added local IP."


with open('/app/nifi-1.2.0/conf/nifi.properties', 'w') as out_file:
    out_file.write(text.replace('nifi.remote.input.socket.port=', 'nifi.remote.input.socket.port=5000'))
print "Changed nifi.properties file. Added HTTP Raw port."


nificommand = subprocess.Popen(['/app/nifi-1.2.0/bin/nifi.sh', 'start'])
print "Starting NiFi."

while True:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    result = sock.connect_ex(('127.0.0.1', 8080))
    if result == 0:
        break
    else:
        time.sleep(4)

print "Starting the platform service"
subprocess.call(['/app/echo_platform_service/echo_platform_service.py', device_uuid, registry_url, mqtt_client, kafka_ip,
                 resource_update_frequency, local_ip, usage_file])
