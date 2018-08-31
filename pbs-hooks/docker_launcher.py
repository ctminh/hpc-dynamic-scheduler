import os
import pbs
import copy
import time
import subprocess

e = pbs.event()
j = e.job
f= open("/home/ctminh/pbs/hooks/logs/" + str(j.id),"w")
f.write("########LAUNCH CONTAINER##########\n")
f.write("Current date time:  %s\n" %time.ctime())
args = copy.deepcopy(e.argv)
pbs.logmsg(pbs.LOG_DEBUG, "[DOCKER]-->args are %s " % e.progname)
f.write("--->args are %s\n" %e.progname)
prog = e.progname
hdir = ""
edir = ""
def add_resource_restriction (call):
	n = j.Resource_List["ncpus"]
	m = j.Resource_List["mem"]
	if n:
		call += " -c " + str(n)
	if m:
		call += " -m " + str(m)
	return call
def add_env (call):
	for i in e.env:
		call += " -e " + str(i)+"="+e.env[i]
	return call
def get_pbs_dirs ():
	global hdir
	global edir
	#get PBS_HOME & PBS_EXEC directories
	hdir = pbs.pbs_conf['PBS_HOME']
	edir = pbs.pbs_conf['PBS_EXEC']
	return
def create_container ():
	global hdir
	global edir
	is_ms = j.in_ms_mom()
	docker = ""
	call = ""
	search_container = "docker ps | grep "+str(j.id)+" | grep -v grep"

	docker = os.popen(search_container).read()
	jid = j.id
	pbs.logmsg(pbs.LOG_DEBUG, "Docker container: %s" % docker)
	f.write("Docker container %s\n" % docker)
	if docker:
		pbs.logmsg(pbs.LOG_DEBUG, "Docker container found!")
		f.write("Docker container found!\n")
	else:
		v = str(j.Variable_List)
		vl = v.split(",")
		image = "DOCKER_IMAGE="
		os_image = "None"
		#get image name
		for i in vl:
			pbs.logmsg(pbs.LOG_DEBUG, "Variable is: %s" % i)
			f.write("Variable is: %s\n" % i)
			if i.startswith(image):
				name = i.split("=",1)
				os_image = name[1]
		if "None" in os_image:
# No need to run any Docker instance.... just bail out!
			e.accept()
		pbs.logmsg(pbs.LOG_DEBUG, "Image is: %s" % os_image)
		f.write("Image is: %s\n" %os_image)
		
		call = "docker run -d -it --name " + str(jid)
		
		call = add_resource_restriction(call);
		pbs.logmsg(pbs.LOG_DEBUG, "Resource is : %s" % call)
		f.write("Resource is %s\n" % call)
		
		call = add_env(call);
		pbs.logmsg(pbs.LOG_DEBUG, "Env is : %s" % call)
		f.write("Env is: %s\n" % call)
		
		get_pbs_dirs();
		pbs.logmsg(pbs.LOG_DEBUG, "Directory is : %s" % hdir)
		f.write("Directory is:  %s\n" % hdir)
		
		job_file=hdir+"/mom_priv/jobs/"+str(jid)+".SC"
		call += " -v "+str(hdir)+":"+str(hdir)+" "+" -v "+str(edir)+":"+str(edir)+" -v /opt:/opt"+" -v /home:/home --net=host --privileged=true"
		call += " " + str(os_image) + " /bin/bash"
		pbs.logmsg(pbs.LOG_DEBUG, "Call is : %s" % call)
		f.write("Call is:  %s\n" % call)
		
		os.popen(call)
		for x in range(0, 5):
			s = subprocess.check_output('docker ps', shell=True)
			if s.find(str(jid)) != -1:
				f.write("Docker container is running. No need to restart!\n")
				break
			else:
				os.popen("docker restart " + str(jid))
				f.write("Docker container is not running. Restarted\n")	
	return
def launch_job ():
	global prog
	global hdir
	is_script = prog.find("bash")
	prog = ""
	get_pbs_dirs()
	pbs.logmsg(pbs.LOG_DEBUG," is_script %d" % is_script)
	f.write("is_script %d\n" % is_script)
	if is_script != -1:
		job_file=hdir+"/mom_priv/jobs/"+str(j.id)+".SC"
	else:
		for arg in args:
			prog=prog+" "+arg

	e.progname="/usr/bin/docker"
	e.argv = []
	e.argv.append("docker")
	e.argv.append("exec")
	e.argv.append(str(j.id))
	e.argv.append("/bin/bash")
	e.argv.append("-c")
	executable=""

	if is_script != -1:
		pbs.logmsg(pbs.LOG_DEBUG, "It's a script")
		f.write("It's a scrip\n")
		e.argv.append(job_file)
	else:
		pbs.logmsg(pbs.LOG_DEBUG, "It's an executable")
		f.write("It's an executable\n")
		executable=""
		for arg in args:
			executable+=arg+" "
			pbs.logmsg(pbs.LOG_DEBUG, "arg[1] : %s" % arg)
			f.write("arg[1] : %s\n" % arg)
		e.argv.append(executable)
	return
create_container()
launch_job()
f.write("Done launch_job\n")
f.write("Call e.accept at %s \n" %time.ctime())
e.accept()
