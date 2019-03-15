#!/usr/bin/env python
from __future__ import print_function
import numpy as np
import re
import random
import subprocess
import matplotlib.pyplot as plt
plt.rcdefaults()

# path to the workload file
filename = "../HPCworkloads/CTC-SP2-1996-3.1-cln.swf"

model_num_nodes = []
model_run_times = []
model_submit_times = []
num_tasks_queue = 32
num_tasks_state = 16
earliest_submit = 0
tasks_state_nodes = []
tasks_state_runtimes = []
tasks_state_submit = []
tasks_queue_nodes = []
tasks_queue_runtimes = []
tasks_queue_submit = []

# for mic nodes
model_num_mics = []
tasks_state_mics = []
tasks_queue_mics = []

# for duedate values
model_duedate_times = []
tasks_state_duedate = []
tasks_queue_duedate = []

SECONDS_IN_A_DAY = 86400
SIM_NUM_DAYS = 15
NUM_EXPERIMENTS = 22

slow_fcfs = []
slow_spt = []
slow_lpt = []
slow_wfp3 = []
slow_unicef = []
slow_edd = []
slow_easy = []
slow_c1 = []
slow_c2 = []
slow_c3 = []
slow_c4 = []

""" read input file """
for line in file(filename):
    row = re.split(" +", line.lstrip(" "))
    if row[0].startswith(";"):
        continue

    if int(row[4]) > 0 and int(row[3]) > 0:
        model_run_times.append(int(row[3]))
        model_num_nodes.append(int(row[4]))
        model_submit_times.append(int(row[1]))

############# add mic-jobs for the dataset ###############
dataset_size = len(model_num_nodes)
mic_25_percent_job = 0.25 * dataset_size    # 25%-jobs need mic
mic_50_percent_job = 0.5 * dataset_size     # 50%-jobs need mic
mic_75_percent_job = 0.75 * dataset_size    # 75%-jobs need mic
# number of mic-jobs
num_mic_jobs = round(mic_25_percent_job, 0)   # the number of mic-jobs in the dataset
# num_mic_jobs = round(mic_50_percent_job, 0)   # the number of mic-jobs in the dataset
# num_mic_jobs = round(mic_75_percent_job, 0)   # the number of mic-jobs in the dataset
# use triangular distribution
left,mode,right = 0, 0.75, 1    # ~25%-jobs for mic
# left,mode,right = 0, 0.5, 1    # ~50%-jobs for mic
# left,mode,right = 0, 0.25, 1    # ~75%-jobs for mic
tria_rand = np.random.triangular(left, mode, right, dataset_size)
count_zero_values = 0
count_one_values = 0
for i in range(0, len(tria_rand)):
    if tria_rand[i] < mode:
        model_num_mics.append(0)
        count_zero_values = count_zero_values + 1
    else:
        model_num_mics.append(1)
        count_one_values = count_one_values + 1
percent_zero = 100.0 * count_zero_values / len(model_num_mics)
percent_one = 100.0 * count_one_values / len(model_num_mics)
print("zero-values: %f%% (count = %d) in model_num_mics - len = %d)" %(percent_zero, count_zero_values, len(model_num_mics)))
print("one-values: %f%% (count = %d) in model_num_mics - len = %d)" %(percent_one, count_one_values, len(model_num_mics)))
# use norm-dist to generate the required number of mic cards in a job
norm_size = count_one_values
mu, sigma = 0.6, 0.1
x = np.random.normal(mu, sigma, size = int(norm_size))
# put dist-values to the dataset
j = 0
for i in xrange(0, len(model_num_mics)):
    if model_num_mics[i] == 1:
        req_mics = x[j] * model_num_nodes[i]
        model_num_mics[i] = int(req_mics)
        j = j + 1
# print("the number of dist-values in model_num_mics: %d" %(j))
# print(model_num_mics[0:100])
# visualize the distribution
# sns.set(color_codes=True)
# sns.distplot(model_num_mics)
# plt.show()

############# add duedate-values for the dataset ###############
# using the model PPW (process time plus wait) 
# Gordon, Valery S. and others. "Due date assignment and scheduling: SLK, TWK and other due date assignment models."
# Production Planning & Control 13.2 (2002): 117-132.
# d[j] = k * p[j] + q (d: duedate, p: processing time, q: slack allowance time)
norm_size = dataset_size
k_mu, k_sigma = 2.2, 0.1
k = np.random.normal(k_mu, k_sigma, size = int(norm_size))
q_mu, q_sigma = 60, 5
q = np.random.normal(q_mu, q_sigma, size = int(norm_size))
# put dist-values to the dataset
for i in xrange(0, len(model_run_times)):
    d = k[i] * model_run_times[i] + q[i]
    model_duedate_times.append(int(d))
# sns.set(color_codes=True)
# sns.distplot(model_duedate_times)
# plt.show()

# estimate timespan
timespan = np.max(model_submit_times) - np.min(model_submit_times)

print('Performing scheduling performance test for the workload trace CTC-SP2-1996-3.1-cln.\n' +
      'Configuration: Using actual runtimes, backfilling disabled')

# generate the job-submission file to run the simulation
choose = 0
for i in xrange(0, NUM_EXPERIMENTS):  # 1e7
    task_file = open("initial-simulation-submit.csv", "w+")
    tasks_state_nodes = []
    tasks_state_mics = []   # for mic-jobs
    tasks_state_runtimes = []
    tasks_state_submit = []
    tasks_state_duedate = []    # for duedate values

    earliest_submit = model_submit_times[choose]
    for j in xrange(0, 16):
        tasks_state_nodes.append(model_num_nodes[choose+j])
        tasks_state_mics.append(model_num_mics[choose+j])   # for mic-jobs
        tasks_state_runtimes.append(model_run_times[choose+j])
        tasks_state_submit.append(model_submit_times[choose+j] - earliest_submit)
        tasks_state_duedate.append(model_duedate_times[choose+j])   # for duedate values
        task_file.write(str(tasks_state_runtimes[j]) + ","
            + str(tasks_state_nodes[j]) + ","
            + str(tasks_state_mics[j]) + ","    # for mic-jobs
            + str(tasks_state_submit[j]) + ","
            + str(tasks_state_duedate[j]) + "\n")    # for duedate values

    tasks_queue_nodes = []
    tasks_queue_mics = []   # for mic-jobs
    tasks_queue_runtimes = []
    tasks_queue_submit = []
    tasks_queue_duedate = []    # for duedate values
    j = 0
    while model_submit_times[choose+num_tasks_state+j] - earliest_submit <= SECONDS_IN_A_DAY * SIM_NUM_DAYS:
        #choose = random.randint(0,len(model_run_times)-1)
        tasks_queue_nodes.append(model_num_nodes[num_tasks_state+choose+j])
        tasks_queue_mics.append(model_num_mics[num_tasks_state+choose+j])   # for mic-jobs
        tasks_queue_runtimes.append(model_run_times[num_tasks_state+choose+j])
        tasks_queue_submit.append(model_submit_times[num_tasks_state+choose+j] - earliest_submit)
        tasks_queue_duedate.append(model_duedate_times[num_tasks_state+choose+j])   # for duedate values
        task_file.write(str(tasks_queue_runtimes[j]) + ","
            + str(tasks_queue_nodes[j]) + ","
            + str(tasks_queue_mics[j]) + ","    # for mic-jobs
            + str(tasks_queue_submit[j]) + ","
            + str(tasks_queue_duedate[j]) + "\n")   # for duedate values
        j = j+1

    task_file.close()
    choose = choose + num_tasks_state + j

    number_of_tasks = len(tasks_queue_runtimes) + len(tasks_state_runtimes)
    print('Performing scheduling experiment %d. Number of tasks=%d' % (i+1, number_of_tasks))

    _buffer = open("plot-temp.dat", "w+")
    subprocess.call(['./sched-simulator-runtime simgrid-xmls/plat_day.xml simgrid-xmls/deployment_ctcsp2.xml -nj ' +
                    str(number_of_tasks)], shell=True, stdout=_buffer)
    subprocess.call(['./sched-simulator-runtime simgrid-xmls/plat_day.xml simgrid-xmls/deployment_ctcsp2.xml -spt -nj ' +
                    str(number_of_tasks)], shell=True, stdout=_buffer)
    subprocess.call(['./sched-simulator-runtime simgrid-xmls/plat_day.xml simgrid-xmls/deployment_ctcsp2.xml -lpt -nj ' +
                    str(number_of_tasks)], shell=True, stdout=_buffer)
    subprocess.call(['./sched-simulator-runtime simgrid-xmls/plat_day.xml simgrid-xmls/deployment_ctcsp2.xml -wfp3 -nj ' +
                    str(number_of_tasks)], shell=True, stdout=_buffer)
    subprocess.call(['./sched-simulator-runtime simgrid-xmls/plat_day.xml simgrid-xmls/deployment_ctcsp2.xml -unicef -nj ' +
                    str(number_of_tasks)], shell=True, stdout=_buffer)
    subprocess.call(['./sched-simulator-runtime simgrid-xmls/plat_day.xml simgrid-xmls/deployment_ctcsp2.xml -edd -nj ' +
                     str(number_of_tasks)], shell=True, stdout=_buffer)
    # subprocess.call(['./sched-simulator-runtime simgrid-xmls/plat_day.xml simgrid-xmls/deployment_ctcsp2.xml -easy -nj ' +
    #                  str(number_of_tasks)], shell=True, stdout=_buffer)
    subprocess.call(['./sched-simulator-runtime simgrid-xmls/plat_day.xml simgrid-xmls/deployment_ctcsp2.xml -f1 -nj ' +
                     str(number_of_tasks)], shell=True, stdout=_buffer)
    subprocess.call(['./sched-simulator-runtime simgrid-xmls/plat_day.xml simgrid-xmls/deployment_ctcsp2.xml -f2 -nj ' +
                     str(number_of_tasks)], shell=True, stdout=_buffer)
    subprocess.call(['./sched-simulator-runtime simgrid-xmls/plat_day.xml simgrid-xmls/deployment_ctcsp2.xml -f3 -nj ' +
                     str(number_of_tasks)], shell=True, stdout=_buffer)
    subprocess.call(['./sched-simulator-runtime simgrid-xmls/plat_day.xml simgrid-xmls/deployment_ctcsp2.xml -f4 -nj ' +
                     str(number_of_tasks)], shell=True, stdout=_buffer)
    _buffer.close()

    _buffer = open("plot-temp.dat", "r")
    lines = list(_buffer)
    slow_fcfs.append(float(lines[1]))
    slow_spt.append(float(lines[3]))
    slow_lpt.append(float(lines[5]))
    slow_wfp3.append(float(lines[7]))
    slow_unicef.append(float(lines[9]))
    slow_edd.append(float(lines[11]))
    # slow_easy.append(float(lines[6]))
    slow_c1.append(float(lines[13]))
    slow_c2.append(float(lines[15]))
    slow_c3.append(float(lines[17]))
    slow_c4.append(float(lines[19]))
    _buffer.close()

# write the test-plt-temp.dat for testing boxplot
# _test_buffer = open("./plot-slowdown/plot-slowdown-ctcsp2-0-mics.dat", "w+")
# _test_buffer = open("./plot-lateness/plot-lateness-ctcsp2-75-mics.dat", "w+")
_test_buffer = open("./plot-throughput/plot-throughput-ctcsp2-25-mics.dat", "w+")
for i in xrange(0, NUM_EXPERIMENTS):
    _test_buffer.write(str(slow_fcfs[i]) + ","
                + str(slow_spt[i]) + ","
                + str(slow_lpt[i]) + ","
                + str(slow_wfp3[i]) + ","
                + str(slow_unicef[i]) + ","
                + str(slow_edd[i]) + ","
                + str(slow_c1[i]) + ","
                + str(slow_c2[i]) + ","
                + str(slow_c3[i]) + ","
                + str(slow_c4[i]) + "\n")
_test_buffer.close()

# policies = ('FCFS', 'SPT', 'LPT', 'WFP3', 'UNI', 'EDD', 'EASY', 'F1', 'F2', 'F3', 'F4')
# policies = ('FCFS', 'SPT', 'LPT', 'WFP3', 'UNI')
policies = ('FCFS', 'SPT', 'LPT', 'WFP3', 'UNI', 'EDD', 'F1', 'F2', 'F3', 'F4')
y_pos = np.arange(len(policies))

performance = []
performance.append(np.mean(slow_fcfs))
performance.append(np.mean(slow_spt))
performance.append(np.mean(slow_lpt))
performance.append(np.mean(slow_wfp3))
performance.append(np.mean(slow_unicef))
performance.append(np.mean(slow_edd))
# performance.append(np.mean(slow_easy))
performance.append(np.mean(slow_c1))
performance.append(np.mean(slow_c2))
performance.append(np.mean(slow_c3))
performance.append(np.mean(slow_c4))

error = []
error.append(np.std(slow_fcfs))
error.append(np.std(slow_spt))
error.append(np.std(slow_lpt))
error.append(np.std(slow_wfp3))
error.append(np.std(slow_unicef))
error.append(np.std(slow_edd))
# error.append(np.std(slow_easy))
error.append(np.std(slow_c1))
error.append(np.std(slow_c2))
error.append(np.std(slow_c3))
error.append(np.std(slow_c4))

# arrange data
all_data = []
all_data.append(slow_fcfs)
all_data.append(slow_spt)
all_data.append(slow_lpt)
all_data.append(slow_wfp3)
all_data.append(slow_unicef)
all_data.append(slow_edd)
# all_data.append(slow_easy)
all_data.append(slow_c1)
all_data.append(slow_c2)
all_data.append(slow_c3)
all_data.append(slow_c4)

# configure the plot
# plt.rc("font", size=45)
# plt.figure(figsize=(16, 14))

# store medians for the boxplot
all_medians = []
xticklabels=['FCFS', 'SPT', 'LPT', 'WFP3', 'UNI', 'EDD', 'C1', 'C2', 'C3', 'C4']

# statistics - medians
print('Experiment Statistics:')
print('Medians:')
i = 0
for m in all_medians:
    print('%s=%.2f' % (xticklabels[i], m))
    i = i+1

# statistics - medians
print('Means:')
i = 0
for p in performance:
    print('%s=%.2f' % (xticklabels[i], p))
    i = i+1

# statistics - medians
print('Standard Deviations:')
i = 0
for e in error:
    print('%s=%.2f' % (xticklabels[i], e))
    i = i+1

# show the end
# print('Boxplot saved in file plots/slowdown-ctcsp2-0-mic.pdf')
print('Boxplot saved in file plot-lateness/')
