#This program is free software; you can redistribute it and/or modify it

from __future__ import print_function
import numpy as np
import re
import random
import subprocess
import sys
import os.path
import datetime
from datetime import timedelta

# filename = "lublin_256.swf"
# filename = "FCFS-2018-03-07.swf"
filename = "FCFS-2018-04-24-Docker.swf"

model_num_nodes = []
model_run_times = []
model_submit_times = []
# model start_time & end_time
model_start_time = []
model_end_time = []
# model mic & duedate
model_mic = []
model_duedate = []
# model job name
model_job_name = []

num_tasks_queue = 32
num_tasks_state = 16
earliest_submit = 0

tasks_state_nodes = []
tasks_state_runtimes = []
tasks_state_submit = []

tasks_queue_nodes = []
tasks_queue_runtimes = []
tasks_queue_submit = []

num_trials = 1280

# Define required cores for each job
required_cores = {"parallelism-sweep-cpu8":8, "threading-misc-histogram":2, "shared-vir-mem":2,
    "nbody-100000":24, "vec-data-cpu4":4, "lu-decomp-100000-mem5":8,
    "lu-decomp-100000-mem20":12, "lu-decomp-100000-mem10":8, "benchmark-offload":2,
    "fft":8, "md":24, "sgefa":12,
    "nbody-700000":12, "mkl-20000":8, "mkl-25000":12,
    "vtune":2, "qe_mic_test":32
}

# Define duedate values for each jobs
duedate = {
    "parallelism-sweep-cpu8":40, "threading-misc-histogram":100, "shared-vir-mem":67,
    "nbody-100000":66, "vec-data-cpu4":69, "lu-decomp-100000-mem5":67,
    "lu-decomp-100000-mem20":160, "lu-decomp-100000-mem10":174, "benchmark-offload":292,
    "fft":1340, "md":956, "sgefa":1525,
    "nbody-700000":2482, "mkl-20000":2605, "mkl-25000":3703,
    "vtune":2323, "qe_mic_test":3533

}

# Define mic for each jobs
required_mic = {
    "parallelism-sweep-cpu8":0, "threading-misc-histogram":0, "shared-vir-mem":1,
    "nbody-100000":0, "vec-data-cpu4":0, "lu-decomp-100000-mem5":0,
    "lu-decomp-100000-mem20":0, "lu-decomp-100000-mem10":0, "benchmark-offload":1,
    "fft":0, "md":0, "sgefa":0,
    "nbody-700000":0, "mkl-20000":0, "mkl-25000":0,
    "vtune":1, "qe_mic_test":1
}

# ////////////////////////////////////////////
# /////// Reading lublin_256.swf ////////////
# ///////////////////////////////////////////

def convDatetimeForm(str_val):
    date = str_val.split(' ')[0]
    time = str_val.split(' ')[1]
    Y = int(date.split('-')[0])
    M = int(date.split('-')[1])
    D = int(date.split('-')[2])
    h = int(time.split(':')[0])
    m = int(time.split(':')[1])
    s = int(time.split(':')[2])

    datetime_form = datetime.datetime(Y, M, D, h, m, s)
    return datetime_form

def convTimetoSeconds(str_val):
    h = int(str_val.split(':')[0])
    m = int(str_val.split(':')[1])
    s = int(str_val.split(':')[2])
    seconds = h*3600 + m*60 + s
    return seconds

# print("------------ Read Log File --------------")
# print("\t\t|name| \t\t |submit_time| \t |cores| |runtimes| \t\t |start_time| \t\t |end_time| \t\t  |mic| \t\t |duedate|")
marked_first_line = 0
for line in file(filename):
    # split values in line
    row = re.split("[\t, \!?]+", line.strip("\n"))
    
    # get submit_time
    if marked_first_line == 0:
        first_queued_time = convDatetimeForm(row[1]+" "+row[2])
    queued_time = convDatetimeForm(row[1]+" "+row[2])
    submit_time = (queued_time - first_queued_time).seconds
    model_submit_times.append(submit_time)
    # print("{}: {}".format(type(queued_time), queued_time))
    # print("typeof(temp) - {}: {}".format(type(temp), temp))

    # get job name
    job_name = row[8]
    model_job_name.append(job_name)
    # print(job_name)

    # get required cores
    req_cores = required_cores.get(job_name)
    model_num_nodes.append(req_cores)
    # print(req_cores)

    # get start_time & end_time
    # start = (convDatetimeForm(row[3]+" "+row[4]) - first_queued_time)
    start_time = (convDatetimeForm(row[3]+" "+row[4]) - first_queued_time).total_seconds()
    model_start_time.append(start_time)

    # end = (convDatetimeForm(row[5]+" "+row[6]) - first_queued_time)
    end_time = (convDatetimeForm(row[5]+" "+row[6]) - first_queued_time).total_seconds()
    model_end_time.append(end_time)

    # runtime = convTimetoSeconds(row[7])
    runtime = (end_time - start_time)
    model_run_times.append(runtime)

    # get model_mic
    req_mic = required_mic.get(job_name)
    model_mic.append(req_mic)

    # get model_duedate
    due_d = duedate.get(job_name) + submit_time
    model_duedate.append(due_d)

    marked_first_line += 1
    # print("%25s \t %5d \t %10d \t %5d \t\t\t %5d \t\t\t %5d \t\t\t %5d \t\t\t %5d" %(job_name, submit_time, req_cores, runtime, start_time, end_time, req_mic, due_d))

""" Checking a list of waiting jobs at each timestamp """
# for i in xrange(0, len(model_submit_times)):
#     num_waiting_job = 0
#     for j in xrange(0, i):
#         if model_submit_times[j] < model_start_time[i] and model_start_time[j] > model_start_time[i]:
#             num_waiting_job += 1
#     print("# of waiting jobs at the time - %d is: %d" %(model_start_time[i], num_waiting_job))
    

# Checking task-set file
start = 0
while os.path.exists("task-sets/set-" + str(start) + ".csv") == True:
    start += 1
# print("After checking task-sets files: start = %d" % start)
# print("-------------------------------------------------------------")

# Maximum number os tuples (S,Q) to be simulated
for i in xrange(start, 2):
    task_file = open("task-sets/set-" + str(i) + ".csv", "w+")
    # check_task_file = open("task-sets/real-task-" + str(i) + ".csv", "w+")
    tasks_state_nodes = []
    tasks_state_runtimes = []
    tasks_state_submit = []
    # add more mic and duedate attribute
    tasks_state_mic = []
    tasks_state_duedate = []
    # add job name to check_task_file
    tasks_state_name = []
    
    # choose a random value
    range_chosen = len(model_run_times) - 1 - (num_tasks_queue + num_tasks_state)
    choose = random.randint(0, range_chosen)
    print("choose a random value in [%d, %d]: choose = %d" % (0, range_chosen, choose))

    # get earliest submit
    earliest_submit = model_submit_times[choose]

    for j in xrange(0, 16):
        tasks_state_nodes.append(model_num_nodes[choose + j])
        tasks_state_runtimes.append(model_run_times[choose + j])
        tasks_state_submit.append(model_submit_times[choose + j] - earliest_submit)
        # add more mic and duedate attribute
        tasks_state_mic.append(model_mic[choose + j])
        tasks_state_duedate.append(model_duedate[choose + j] - earliest_submit)
        # add job name
        tasks_state_name.append(model_job_name[choose + j])

        # write to file - current-simulation.csv
        task_file.write(str(tasks_state_runtimes[j]) + ","
            + str(tasks_state_nodes[j]) + ","
            + str(tasks_state_submit[j]) + ","
            + str(tasks_state_mic[j]) + ","
            + str(tasks_state_duedate[j]) + "\n")
    
    tasks_queue_nodes = []
    tasks_queue_runtimes = []
    tasks_queue_submit = []
    # add more mic and duedate attribute
    tasks_queue_mic = []
    tasks_queue_duedate = []
    # add job name
    tasks_queue_name = []

    # print("num_tasts_state = %d, choose = %d" % (num_tasks_state, choose))
    
    for j in xrange(0, 32):
        tasks_queue_nodes.append(model_num_nodes[num_tasks_state + choose + j])
        tasks_queue_runtimes.append(model_run_times[num_tasks_state + choose + j])
        tasks_queue_submit.append(model_submit_times[num_tasks_state + choose + j] - earliest_submit)
        # add more mic and duedate attribute
        tasks_queue_mic.append(model_mic[num_tasks_state + choose + j])
        tasks_queue_duedate.append(model_duedate[num_tasks_state + choose + j] - earliest_submit)
        tasks_queue_name.append(model_job_name[num_tasks_state + choose + j])

        # write to file
        task_file.write(str(tasks_queue_runtimes[j]) + ","
            + str(tasks_queue_nodes[j]) + ","
            + str(tasks_queue_submit[j]) + ","
            + str(tasks_queue_mic[j]) + ","
            + str(tasks_queue_duedate[j]) + "\n")

    task_file.close()

    # permutation steps for creating dataset
    shape = (num_trials, num_tasks_queue)
    perm_indices = np.empty(shape, dtype = int)

    for j in xrange(0, num_trials):
        perm_indices[j] = np.arange(32)

    # prepare a list of jobs for simulation
    subprocess.call(['cp task-sets/set-' + str(i) + '.csv' ' current-simulation.csv'], shell = True)

    # simulate job submission
    subprocess.call(['./trials_simulator simple_cluster.xml deployment_cluster.xml -state > states/set' + str(i) + '.csv'], shell = True)

    # check result temp data file
    if(os.path.exists("result-temp.dat") == True):
        subprocess.call(['rm result-temp.dat'], shell=True)

    # check training-data set
    if(os.path.exists("training-data/set-"+str(i)+".csv") == True):
        subprocess.call(['rm training-data/set-'+str(i)+'.csv'], shell=True)  

    # shuffle tasks in queues
    shuffle_tasks_queue_runtimes = np.copy(tasks_queue_runtimes)
    shuffle_tasks_queue_nodes = np.copy(tasks_queue_nodes)
    shuffle_tasks_queue_submit = np.copy(tasks_queue_submit)
    shuffle_tasks_queue_mic = np.copy(tasks_queue_mic)
    shuffle_tasks_queue_duedate = np.copy(tasks_queue_duedate)

    # the process of permutaion
    for j in xrange(0, num_trials):
        # iteration file
        iteration_file = open("current-simulation.csv", "w+")

        for k in xrange(0, 32):
            choose = random.randint(0, 31)
            # permute the position of shuffle_tasks_queues
            buffer_runtimes = shuffle_tasks_queue_runtimes[choose]
            buffer_nodes = shuffle_tasks_queue_nodes[choose]
            buffer_submits = shuffle_tasks_queue_submit[choose]
            buffer_mic = shuffle_tasks_queue_mic[choose]
            buffer_duedate = shuffle_tasks_queue_duedate[choose]

            shuffle_tasks_queue_runtimes[choose] = shuffle_tasks_queue_runtimes[k]
            shuffle_tasks_queue_nodes[choose] = shuffle_tasks_queue_nodes[k]
            shuffle_tasks_queue_submit[choose] = shuffle_tasks_queue_submit[k]
            shuffle_tasks_queue_mic[choose] = shuffle_tasks_queue_mic[k]
            shuffle_tasks_queue_duedate[choose] = shuffle_tasks_queue_duedate[k]

            shuffle_tasks_queue_runtimes[k] = buffer_runtimes
            shuffle_tasks_queue_nodes[k] = buffer_nodes
            shuffle_tasks_queue_submit[k] = buffer_submits
            shuffle_tasks_queue_mic[k] = buffer_mic
            shuffle_tasks_queue_duedate[k] = buffer_duedate

            # get the buffer index
            buffer_index = perm_indices[j, choose]

            # swap perm_indices and buffer_index
            perm_indices[j, choose] = perm_indices[j, k]
            perm_indices[j, k] = buffer_index

        for k in xrange(0, 16):
            iteration_file.write(str(tasks_state_runtimes[k]) + "," + str(tasks_state_nodes[k]) + "," + str(tasks_state_submit[k]) + "," + str(tasks_state_mic[k]) + "," + str(tasks_state_duedate[k]) + "\n")

        for k in xrange(0, 32):
            iteration_file.write(str(tasks_queue_runtimes[perm_indices[j,k]]) + "," + str(tasks_queue_nodes[perm_indices[j,k]]) + "," + str(tasks_queue_submit[perm_indices[j,k]]) + "," + str(tasks_queue_mic[perm_indices[j,k]]) + "," + str(tasks_queue_duedate[perm_indices[j,k]]) + "\n")
        
        iteration_file.close()
        
        subprocess.call(['./trials_simulator simple_cluster.xml deployment_cluster.xml >> result-temp.dat'], shell=True)
    
    # init arrays to store distributions of result
    output = ""
    exp_sum_slowdowns = 0.0
    distribution = np.zeros(num_tasks_queue)
    exp_first_choice = np.zeros((num_trials), dtype = np.int32)
    exp_slowdowns = np.zeros(num_trials)
    state = ""

    # prefix
    states_prefix = "states/set"

    # print("---------- Analyze the results -------------")
    for trialID in xrange(0, num_trials):
        exp_first_choice[trialID] = perm_indices[trialID, 0]

    trialID = 0
    result_file = open("result-temp.dat", "r")
    lines = result_file.readlines()
    
    if len(lines) != num_trials:
        result_file.close()
        i = i - 1
        continue

    for line in lines:
        exp_slowdowns[trialID] = float(line)
        exp_sum_slowdowns += float(line)
        trialID = trialID + 1
    result_file.close()

    for line in file(states_prefix + str(i) + ".csv"):
        state = str(line)

    for trialID in xrange(0, len(exp_slowdowns)):
        distribution[exp_first_choice[trialID]] += exp_slowdowns[trialID]

    # normalize the result
    for k in xrange(0, len(distribution)):
        distribution[k] = distribution[k] / exp_sum_slowdowns

    for k in xrange(0, num_tasks_queue):
        output += str(int(tasks_queue_runtimes[k])) + "," + str(int(tasks_queue_nodes[k])) + "," + str(int(tasks_queue_submit[k])) + "," + str(int(tasks_queue_mic[k])) + "," + str(int(tasks_queue_duedate[k])) + ","
        output += str(distribution[k]) + "\n"

    out_file = open("training-data/set-" + str(i) + ".csv", "w+")
    out_file.write(output)
    out_file.close()


    # line break for display
    print("-------------------------------------------------------------")
