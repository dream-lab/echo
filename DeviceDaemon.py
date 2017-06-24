from __future__ import absolute_import, print_function, division
import time
import platform
import subprocess
import json
import requests

url = 'http://13.71.125.147:8080/cat'
device_uuid = "1"

##############  COMMENT THE NEXT 9 LINES WHILE RUNNING ON RASPBERRY PI  #####################
# host_cpu = 0.0
# host_mem = {'total': 923, 'percent_cached': 12, 'used': 689, 'percent': 74}
# container_mem = 12
# host_uptime_value = {'day': 3, 'time': '2:40'}
# host_distro = "debian 8.0 -"
# host_disk = {'total': '15G', 'percent': '67%', 'free': '4.8G', 'used': '9.3G'}
# container = "nifi-2  RUNNING  10.0.3.21  -     YES"
# host_ip_addr = "10.24.24.219"
# name = "nifi-2"


def memory_usage(name):
    """
    returns memory usage in MB
    """
    cmd = ['lxc-cgroup -n %s memory.usage_in_bytes' % name]
    try:
        out = subprocess.check_output(cmd, shell=True).splitlines()
    except subprocess.CalledProcessError:
        return 0
    return int(int(out[0]) / 1024 / 1024)
def host_memory_usage():
    """
    returns a dict of host memory usage values
                    {'percent': int((used/total)*100),
                    'percent_cached':int((cached/total)*100),
                    'used': int(used/1024),
                    'total': int(total/1024)}
    """
    total, free, buffers, cached = 0, 0, 0, 0
    with open('/proc/meminfo') as out:
        for line in out:
            parts = line.split()
            key = parts[0]
            value = parts[1]
            if key == 'MemTotal:':
                total = float(value)
            if key == 'MemFree:':
                free = float(value)
            if key == 'Buffers:':
                buffers = float(value)
            if key == 'Cached:':
                cached = float(value)
    used = (total - (free + buffers + cached))
    return {'percent': int((used / total) * 100),
            'percent_cached': int((cached / total) * 100),
            'used': int(used / 1024),
            'total': int(total / 1024)}
def host_cpu_percent():
    """
    returns CPU usage in percent
    """
    with open('/proc/stat', 'r') as f:
        line = f.readlines()[0]

    data = line.split()
    previdle = float(data[4])
    prevtotal = float(data[1]) + float(data[2]) + float(data[3]) + float(data[4])
    time.sleep(0.1)

    with open('/proc/stat', 'r') as f:
        line = f.readlines()[0]

    data = line.split()
    idle = float(data[4])
    total = float(data[1]) + float(data[2]) + float(data[3]) + float(data[4])
    intervaltotal = total - prevtotal
    percent = int(100 * (intervaltotal - (idle - previdle)) / intervaltotal)
    return str('%.1f' % percent)
def host_disk_usage(partition=None):
    """
     returns a dict of disk usage values
                     {'total': usage[1],
                     'used': usage[2],
                     'free': usage[3],
                     'percent': usage[4]}
     """
    # partition = lxcdir()
    partition = "/var/lib/lxc/"
    usage = subprocess.check_output(['df -h %s' % partition], shell=True).split('\n')[1].split()
    return {'total': usage[1],
            'used': usage[2],
            'free': usage[3],
            'percent': usage[4]}
def host_uptime():
    """
    returns a dict of the system uptime
            {'day': days,
            'time': '%d:%02d' % (hours,minutes)}
    """
    with open('/proc/uptime') as f:
        uptime = int(f.readlines()[0].split('.')[0])
    minutes = int(uptime / 60) % 60
    hours = int(uptime / 60 / 60) % 24
    days = int(uptime / 60 / 60 / 24)
    return {'day': days,
            'time': '%d:%02d' % (hours, minutes)}
def name_distro():
    """
    return the System version
    """
    dist = '%s %s - %s' % platform.linux_distribution()

    return dist
def container_list():
    """
    returns list of containers running
    """
    cmd = ['lxc-ls --fancy']
    try:
        out = subprocess.check_output(cmd, shell=True).splitlines()
    except subprocess.CalledProcessError:
        return 0
    return (out[2])
def host_ip():
    """
    returns list of containers running
    """
    cmd = ['ifconfig | grep "inet addr"']
    try:
        out = subprocess.check_output(cmd, shell=True).splitlines()
    except subprocess.CalledProcessError:
        return 0
    return (out[0].split()[1].split(":")[1])


##############  UNCOMMENT THE NEXT 9 LINES WHILE RUNNING ON RASPBERRY PI  #####################
container = (container_list())
host_cpu = (host_cpu_percent())
host_mem = (host_memory_usage())
name=(container.split()[0])
container_mem = (memory_usage(name))
host_uptime_value = (host_uptime())
host_distro = (name_distro())
host_disk = (host_disk_usage())
host_ip_addr = (host_ip())

# data = {"host_ip": host_ip_addr, "host_distro": host_distro, "container_name": name,"container_ip": container.split()[2], "host_cpu": host_cpu, "host_mem": host_mem, "host_disk": host_disk, "container_mem": container_mem, "host_uptime": host_uptime_value}

cpu_data = {"item-metadata": [
    {
        "val": "CPU Meta Data",
        "rel": "urn:X-hypercat:rels:hasDescription:en"
    },
    {
        "val": str(host_cpu),
        "rel": "CPUUtil"
    }
],
    "href": "/device/cpu/" + device_uuid
}

mem_data = {"item-metadata": [
    {
        "val": "Memory Meta Data",
        "rel": "urn:X-hypercat:rels:hasDescription:en"
    },
    {
        "val": str(host_mem['used']),
        "rel": "MemUtil"
    }
],
    "href": "/device/mem/" + device_uuid
}


final_cpu_url=url+"?href=/device/cpu/"+device_uuid
final_mem_url=url+"?href=/device/mem/"+device_uuid

#r1 = requests.delete(final_cpu_url)
#time.sleep(1)
#r2 = requests.delete(final_mem_url)
#time.sleep(1)

headers = {'Content-type': 'text/plain', 'Accept-Language': 'en-US,en;q=0.5'}

r3 = requests.post(final_mem_url, data=json.dumps(mem_data), headers=headers)

time.sleep(1)

r4 = requests.post(final_cpu_url, data=json.dumps(cpu_data), headers=headers)


#print(r1.text)
#print(r1.status_code)
#print(r2.text)
#print(r2.status_code)
print(r3.text)
print(r3.status_code)
print(r4.text)
print(r4.status_code)

