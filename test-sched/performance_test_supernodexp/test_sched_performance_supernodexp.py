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
import time

import matplotlib.pyplot as plt
plt.rcdefaults()
import matplotlib as mpl

filename = "../HPCworkloads/FCFS-2018-04-24-Docker.swf"

SIM_NUM_DAYS = 1
SECONDS_IN_A_DAY = 86400
NUM_EXPERIMENTS = 15
WARM_UP_QUEUE_SIZE = 16
RUNNING_QUEUE_SIZE = 32

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

num_tasks_queue = RUNNING_QUEUE_SIZE
num_tasks_state = WARM_UP_QUEUE_SIZE
earliest_submit = 0

tasks_state_nodes = []
tasks_state_runtimes = []
tasks_state_submit = []

tasks_queue_nodes = []
tasks_queue_runtimes = []
tasks_queue_submit = []

# store slowdowns
slowdown_fcfs = []
slowdown_spt = []
slowdown_lpt = []
slowdown_wfp3 = []
slowdown_unicef = []
slowdown_edd = []
slowdown_c1 = []
slowdown_c2 = []
slowdown_c3 = []
slowdown_c4 = []

num_trials = 128000

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
count_mic = 0
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
    if req_mic == 1:
        count_mic = count_mic + 1

    # get model_duedate
    due_d = duedate.get(job_name) + submit_time
    model_duedate.append(due_d)

    marked_first_line += 1

# print("Job name \t\t\t submit_time  req_cores   runtime \t\t start_time \t\t end_time \t\t req_mic \t\t duedate\n")
# for i in xrange(0,10):
#     print("%25s \t %5d \t %10d \t %5d \t\t\t %5d \t\t\t %5d \t\t\t %5d \t\t\t %5d" %(
#         model_job_name[i], model_submit_times[i], model_num_nodes[i], model_run_times[i], model_start_time[i], model_end_time[i], model_mic[i], model_duedate[i]))

print('Performing scheduling performance test for the workload trace SuperNode-XP in HCMUT 2018.\n'+
     'Configuration: Using processing time estimates, backfilling enabled')

percentage_mic_jobs = 100 * (float(count_mic) / float(len(model_submit_times)))
percentage_cpu_jobs = 100 - percentage_mic_jobs
print('+ MIC jobs: %.2f' %(percentage_mic_jobs))
print('+ CPU jobs: %.2f' %(percentage_cpu_jobs))

choose = 0
for i in xrange(0, NUM_EXPERIMENTS):
    task_file = open("initial-simulation-submit.csv", "w+")
    
    # submit jobs for warming up state
    jobs_state_runtimes = []
    jobs_state_cores = []
    jobs_state_submit = []
    jobs_state_mic = []
    jobs_state_duedate = []
    # jobs_state_req_runtimes = []

    # mark the first job
    earliest_submitted_job = model_submit_times[choose]

    # write jobs in the warm-up state
    for j in xrange(0, WARM_UP_QUEUE_SIZE):
        jobs_state_runtimes.append(model_run_times[choose + j])
        jobs_state_cores.append(model_num_nodes[choose + j])
        jobs_state_submit.append(model_submit_times[choose + j] - earliest_submitted_job)
        jobs_state_mic.append(model_mic[choose + j])
        jobs_state_duedate.append(model_duedate[choose + j])
        task_file.write(str(jobs_state_runtimes[j]) + ","
            + str(jobs_state_cores[j]) + ","
            + str(jobs_state_submit[j]) + ","
            + str(jobs_state_mic[j]) + ","
            + str(jobs_state_duedate[j]) + "\n")

    # submit jobs for evaluating state
    jobs_queue_runtimes = []
    jobs_queue_cores = []
    jobs_queue_submit = []
    jobs_queue_mic = []
    jobs_queue_duedate = []
    j = 0
    # while model_submit_times[choose + num_tasks_state + j] - earliest_submit <= 3600: # submitted time in ~ 1 hours
    while j < 112:
        jobs_queue_runtimes.append(model_run_times[choose + num_tasks_state + j])
        jobs_queue_cores.append(model_num_nodes[choose + num_tasks_state + j])
        jobs_queue_submit.append(model_submit_times[choose + num_tasks_state + j] - earliest_submitted_job)
        jobs_queue_mic.append(model_mic[choose + num_tasks_state + j])
        jobs_queue_duedate.append(model_duedate[choose + num_tasks_state + j])
        task_file.write(str(jobs_queue_runtimes[j]) + ","
            + str(jobs_queue_cores[j]) + ","
            + str(jobs_queue_submit[j]) + ","
            + str(jobs_queue_mic[j]) + ","
            + str(jobs_queue_duedate[j]) + "\n")
        j = j + 1
    task_file.close()

    # increase the file
    print("choose = %d" %(choose))
    choose = choose + num_tasks_state + j

    # number of submitted jobs
    number_of_jobs = len(jobs_state_runtimes) + len(jobs_queue_runtimes) 
    print('Performing scheduling experiment %d. Number of jobs = %d' %(i+1, number_of_jobs))

    # run simulation and plot the results
    _buffer = open("plot-temp.dat", "w+")
    subprocess.call(['./mic-cluster-simulator simple_cluster.xml deployment_cluster.xml -nj ' + str(number_of_jobs)], shell = True, stdout = _buffer)
    subprocess.call(['./mic-cluster-simulator simple_cluster.xml deployment_cluster.xml -lpt -nj ' + str(number_of_jobs)], shell = True, stdout = _buffer)
    subprocess.call(['./mic-cluster-simulator simple_cluster.xml deployment_cluster.xml -spt -nj ' + str(number_of_jobs)], shell = True, stdout = _buffer)
    subprocess.call(['./mic-cluster-simulator simple_cluster.xml deployment_cluster.xml -wfp3 -nj ' + str(number_of_jobs)], shell = True, stdout = _buffer)
    subprocess.call(['./mic-cluster-simulator simple_cluster.xml deployment_cluster.xml -unicef -nj ' + str(number_of_jobs)], shell = True, stdout = _buffer)
    subprocess.call(['./mic-cluster-simulator simple_cluster.xml deployment_cluster.xml -edd -nj ' + str(number_of_jobs)], shell = True, stdout = _buffer)
    subprocess.call(['./mic-cluster-simulator simple_cluster.xml deployment_cluster.xml -c1 -nj ' + str(number_of_jobs)], shell = True, stdout = _buffer)
    subprocess.call(['./mic-cluster-simulator simple_cluster.xml deployment_cluster.xml -c2 -nj ' + str(number_of_jobs)], shell = True, stdout = _buffer)
    subprocess.call(['./mic-cluster-simulator simple_cluster.xml deployment_cluster.xml -c3 -nj ' + str(number_of_jobs)], shell = True, stdout = _buffer)
    subprocess.call(['./mic-cluster-simulator simple_cluster.xml deployment_cluster.xml -c4 -nj ' + str(number_of_jobs)], shell = True, stdout = _buffer)
    _buffer.close()

    _buffer = open("plot-temp.dat", "r")
    lines = list(_buffer)
    slowdown_fcfs.append(float(lines[1]))
    slowdown_spt.append(float(lines[3]))
    slowdown_lpt.append(float(lines[5]))
    slowdown_wfp3.append(float(lines[7]))
    slowdown_unicef.append(float(lines[9]))
    slowdown_edd.append(float(lines[11]))
    slowdown_c1.append(float(lines[13]))
    slowdown_c2.append(float(lines[15]))
    slowdown_c3.append(float(lines[17]))
    slowdown_c4.append(float(lines[19]))
    _buffer.close()

# write the test-plt-temp.dat for testing boxplot
# _test_buffer = open("test-plot-temp.dat", "w+")
# for i in xrange(0, NUM_EXPERIMENTS):
#     _test_buffer.write(str(slowdown_fcfs[i]) + ","
#                 + str(slowdown_spt[i]) + ","
#                 + str(slowdown_lpt[i]) + ","
#                 + str(slowdown_wfp3[i]) + ","
#                 + str(slowdown_unicef[i]) + ","
#                 + str(slowdown_edd[i]) + ","
#                 + str(slowdown_c1[i]) + ","
#                 + str(slowdown_c2[i]) + ","
#                 + str(slowdown_c3[i]) + ","
#                 + str(slowdown_c4[i]) + "\n")
# _test_buffer.close()

performance = []
performance.append(np.mean(slowdown_fcfs))
performance.append(np.mean(slowdown_spt))
performance.append(np.mean(slowdown_lpt))
performance.append(np.mean(slowdown_wfp3))
performance.append(np.mean(slowdown_unicef))
performance.append(np.mean(slowdown_edd))
performance.append(np.mean(slowdown_c1))
performance.append(np.mean(slowdown_c2))
performance.append(np.mean(slowdown_c3))
performance.append(np.mean(slowdown_c4))

error = []
error.append(np.std(slowdown_fcfs))
error.append(np.std(slowdown_spt))
error.append(np.std(slowdown_lpt))
error.append(np.std(slowdown_wfp3))
error.append(np.std(slowdown_unicef))
error.append(np.std(slowdown_edd))
error.append(np.std(slowdown_c1))
error.append(np.std(slowdown_c2))
error.append(np.std(slowdown_c3))
error.append(np.std(slowdown_c4))

# draw the graph
plt.rc("font", size=45)
plt.figure(figsize=(16,14))

# arrange data
all_data = []
all_data.append(slowdown_fcfs)
all_data.append(slowdown_spt)
all_data.append(slowdown_lpt)
all_data.append(slowdown_wfp3)
all_data.append(slowdown_unicef)
all_data.append(slowdown_edd)
all_data.append(slowdown_c1)
all_data.append(slowdown_c2)
all_data.append(slowdown_c3)
all_data.append(slowdown_c4)

# medians for the boxplot
all_medians = []

# create axes for the boxplot
axes = plt.axes()

# converting data to the numpy format
np_converted_data = np.array(all_data)

# for drawing arrows which dedicate the out values on the box plot
OUT_POLICIES = 10
MAX_OUT = [1,1,1,1,1,1,1,1,1,1]
outliers = np.zeros((OUT_POLICIES,max(MAX_OUT))) # a list: 6x2 [[ 0.  0.], [ 0.   0.], ...]

for i in range(0,OUT_POLICIES):
    temp = np_converted_data[i,:] # get: fcfs, spt, lpt, wfp3, unicef, edd
    for j in range(0,MAX_OUT[i]):
        _max = np.max(temp) # get max: fcfs, spt, lpt, wfp3, unicef, edd (run 2-loop)
        outliers[i,j] = _max
        temp = np.delete(temp, np.argmax(temp)) # why: the last one: it is deleted 2 max-values

# create xsticks
xticks = [y+1 for y in range(len(all_data))] # the number of labels for the x-axis

# draw the value-points of each scheduling-algorithm with the positions on x-axis
plt.plot(xticks[0:1], np_converted_data[0:1], 'o', color='darkorange')
plt.plot(xticks[1:2], np_converted_data[1:2], 'o', color='darkorange')
plt.plot(xticks[2:3], np_converted_data[2:3], 'o', color='darkorange')
plt.plot(xticks[3:4], np_converted_data[3:4], 'o', color='darkorange')
plt.plot(xticks[4:5], np_converted_data[4:5], 'o', color='darkorange')
plt.plot(xticks[5:6], np_converted_data[5:6], 'o', color='darkorange')
plt.plot(xticks[6:7], np_converted_data[6:7], 'o', color='darkorange')
plt.plot(xticks[7:8], np_converted_data[7:8], 'o', color='darkorange')
plt.plot(xticks[8:9], np_converted_data[8:9], 'o', color='darkorange')
plt.plot(xticks[9:10], np_converted_data[9:10], 'o', color='darkorange')

# set the y-axis lim
# plt.ylim((0, 220))

# x_offset and y_low ???
x_offset = [0.255, 0.255, 0.255, 0.255, 0.255, 0.255, 0.255, 0.255, 0.255, 0.255]
y_low = [25, 25, 25, 25, 25, 25, 25, 25, 25, 25]

# outliers now
# [[ 100.182382    0.      ]
#  [ 199.393931    0.      ]
#  [  32.472593    0.      ]
#  [  28.471696    0.      ]
#  [  30.08633     0.      ]
#  [  20.354792    0.      ]
#  [        ...            ]]

# for i in range(0, OUT_POLICIES):
#     output = '%.1f' % (outliers[i,0])
#     plt.annotate(output, xy=(xticks[i], 27), xytext=(xticks[i]-x_offset[i], y_low[i]),
#                         arrowprops=dict(facecolor='black', shrink=0.05),fontsize=24)

# medians for the boxplot
for p in all_data:
    all_medians.append(np.median(p))

# plot box plot
plt.boxplot(all_data, showfliers=False)

# adding horizontal grid lines
# for ax in axes:
axes.yaxis.grid(True)
axes.set_xticks([y+1 for y in range(len(all_data))])

# add x-tick labels
xticklabels=['FCFS', 'SPT', 'LPT', 'WFP3', 'UNI', 'EDD', 'C1', 'C2', 'C3', 'C4']
plt.setp(axes, xticks=[y+1 for y in range(len(all_data))], xticklabels=['FCFS', 'SPT', 'LPT', 'WFP3', 'UNI', 'EDD', 'C1', 'C2', 'C3', 'C4'])

plt.tick_params(axis='both', which='major', labelsize=28)
plt.tick_params(axis='both', which='minor', labelsize=28)

# save image to file
plt.savefig('plots/supernode-xp.pdf', format='pdf', dpi=1000, bbox_inches='tight')
print('Boxplot saved in file supernode-xp.pdf')

# plt.show()

print('Experiment Statistics:')
print('Medians:')
i=0
for m in all_medians:
    print('%s=%.2f' % (xticklabels[i],m))
    i=i+1

print('Means:')
i=0
for p in performance:
    print('%s=%.2f' % (xticklabels[i],p))
    i=i+1

print('Standard Deviations:')
i=0
for e in error:
    print('%s=%.2f' % (xticklabels[i],e))
    i=i+1

print('Boxplot saved in file supernode-xp.pdf')

#print("%d" % random.randint(2,8))