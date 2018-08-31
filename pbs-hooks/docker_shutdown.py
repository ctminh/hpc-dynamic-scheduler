import pbs
import os
import time
import subprocess

e = pbs.event()
j = e.job

f= open("/home/ctminh/pbs/hooks/logs/" + str(j.id),"a")
f.write("#########STOP CONTAINER########\n")
f.write("Start date time:  %s\n" %time.ctime())

jid = j.id
v = str(j.Variable_List)
vl = v.split(",")
image="DOCKER_IMAGE="
os_image="None"
for i in vl:
	pbs.logmsg(pbs.LOG_DEBUG, "Variable is: %s" % i)
	f.write("Variable is: %s\n" %i)
	if i.startswith(image):
		name = i.split("=",1)
		os_image = name[1]
if "None" in os_image:
	e.accept()
# Find out if the container is running
#s = subprocess.check_output('docker ps', shell=True)
#if s.find(str(jid)) != -1:
#	f.write("Docker container is running. stop it\n")
#	call = "docker stop " + str(jid)
#	pbs.logmsg(pbs.LOG_DEBUG, "Call is : %s" %call)
#	f.write("Call is: %s\n" %call)
#	os.system(call)
#else:
#	f.write("ERROR : Docker container stoped!\n")

# Call and wait untill done
#call = "docker stop " + str(jid)
#pbs.logmsg(pbs.LOG_DEBUG, "Call is : %s" %call)
#f.write("Call is: %s\n" %call)
#call = "nohup docker rm -f " + str(jid) + " &"
#os.system(call)
#f.write("Call is: %s\n" %call)

# Doesn't work cause pbs kill subprocesses after 
subprocess.Popen(['docker', 'rm', '-f', '{}'.format(str(jid))], preexec_fn=os.setsid)
#time.sleep(20)

f.write("Hook end at : %s \n" %time.ctime())
e.accept()
