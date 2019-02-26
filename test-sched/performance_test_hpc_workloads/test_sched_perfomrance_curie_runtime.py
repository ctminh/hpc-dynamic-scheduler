#!/usr/bin/env python
from __future__ import print_function
import numpy as np
import re
import random
import subprocess
import matplotlib.pyplot as plt
plt.rcdefaults()

filename = "../HPCworkloads/CEA-Curie-2011-2.1-cln.swf"

_i = 0  # i for something
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

SECONDS_IN_A_DAY = 86400
SIM_NUM_DAYS = 15
NUM_EXPERIMENTS = 15

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
    
    if int(row[4]) >= 1 and int(row[3]) >= 1:
        model_run_times.append(int(row[3]))
        model_num_nodes.append(int(row[4]))
        model_submit_times.append(int(row[1]))

# estimate timespan
timespan = np.max(model_submit_times) - np.min(model_submit_times)

print('Performing scheduling performance test for the workload trace CEA-Curie-2011-2.1-cln.\n' +
      'Configuration: Using actual runtimes, backfilling disabled')

# generate the job-submission file to run the simulation
choose = 0
for i in xrange(0, NUM_EXPERIMENTS):  # 1e7
    task_file = open("initial-simulation-submit.csv", "w+")
    tasks_state_nodes = []
    tasks_state_runtimes = []
    tasks_state_submit = []
    earliest_submit = model_submit_times[choose]
    for j in xrange(0, 16):
        tasks_state_nodes.append(model_num_nodes[choose+j])
        tasks_state_runtimes.append(model_run_times[choose+j])
        tasks_state_submit.append(model_submit_times[choose+j] - earliest_submit)
        task_file.write(str(tasks_state_runtimes[j]) + ","
            + str(tasks_state_nodes[j]) + ","
            + str(tasks_state_submit[j]) + "\n")

    tasks_queue_nodes = []
    tasks_queue_runtimes = []
    tasks_queue_submit = []
    j = 0
    while model_submit_times[choose+num_tasks_state+j] - earliest_submit <= SECONDS_IN_A_DAY * SIM_NUM_DAYS:
        tasks_queue_nodes.append(model_num_nodes[num_tasks_state+choose+j])
        tasks_queue_runtimes.append(model_run_times[num_tasks_state+choose+j])
        tasks_queue_submit.append(model_submit_times[num_tasks_state+choose+j] - earliest_submit)
        task_file.write(str(tasks_queue_runtimes[j]) + ","
            + str(tasks_queue_nodes[j]) + ","
            + str(tasks_queue_submit[j]) + "\n")
        j = j+1
    task_file.close()
    choose = choose + num_tasks_state + j

    # skipping heavy underutilized/highly abnormal(in all policies) sequences
    if i == 7 or i == 0:
        continue

    number_of_tasks = len(tasks_queue_runtimes) + len(tasks_state_runtimes)
    print('Performing scheduling experiment %d. Number of tasks=%d' % (_i+1, number_of_tasks))

    _buffer = open("plot-temp.dat", "w+")

    subprocess.call(['./sched-simulator-runtime simgrid-xmls/plat_day.xml simgrid-xmls/deployment_curie.xml -nj ' +
                     str(number_of_tasks)], shell=True, stdout=_buffer)
    subprocess.call(['./sched-simulator-runtime simgrid-xmls/plat_day.xml simgrid-xmls/deployment_curie.xml -spt -nj ' +
                     str(number_of_tasks)], shell=True, stdout=_buffer)
    subprocess.call(['./sched-simulator-runtime simgrid-xmls/plat_day.xml simgrid-xmls/deployment_curie.xml -lpt -nj ' +
                     str(number_of_tasks)], shell=True, stdout=_buffer)
    subprocess.call(['./sched-simulator-runtime simgrid-xmls/plat_day.xml simgrid-xmls/deployment_curie.xml -wfp3 -nj ' +
                     str(number_of_tasks)], shell=True, stdout=_buffer)
    subprocess.call(['./sched-simulator-runtime simgrid-xmls/plat_day.xml simgrid-xmls/deployment_curie.xml -unicef -nj ' +
                     str(number_of_tasks)], shell=True, stdout=_buffer)
    # subprocess.call(['./sched-simulator-runtime simgrid-xmls/plat_day.xml simgrid-xmls/deployment_curie.xml -edd -nj ' +
    #                  str(number_of_tasks)], shell=True, stdout=_buffer)
    # subprocess.call(['./sched-simulator-runtime simgrid-xmls/plat_day.xml simgrid-xmls/deployment_curie.xml -easy -nj ' +
    #                  str(number_of_tasks)], shell=True, stdout=_buffer)
    # subprocess.call(['./sched-simulator-runtime simgrid-xmls/plat_day.xml simgrid-xmls/deployment_curie.xml -f1 -nj ' +
    #                  str(number_of_tasks)], shell=True, stdout=_buffer)
    # subprocess.call(['./sched-simulator-runtime simgrid-xmls/plat_day.xml simgrid-xmls/deployment_curie.xml -f2 -nj ' +
    #                  str(number_of_tasks)], shell=True, stdout=_buffer)
    # subprocess.call(['./sched-simulator-runtime simgrid-xmls/plat_day.xml simgrid-xmls/deployment_curie.xml -f3 -nj ' +
    #                  str(number_of_tasks)], shell=True, stdout=_buffer)
    # subprocess.call(['./sched-simulator-runtime simgrid-xmls/plat_day.xml simgrid-xmls/deployment_curie.xml -f4 -nj ' +
    #                  str(number_of_tasks)], shell=True, stdout=_buffer)
    _buffer.close()

    _buffer = open("plot-temp.dat", "r")
    lines = list(_buffer)
    slow_fcfs.append(float(lines[0]))
    slow_spt.append(float(lines[1]))
    slow_lpt.append(float(lines[2]))
    slow_wfp3.append(float(lines[3]))
    slow_unicef.append(float(lines[4]))
    # slow_edd.append(float(lines[5]))
    # slow_easy.append(float(lines[6]))
    # slow_c1.append(float(lines[7]))
    # slow_c2.append(float(lines[8]))
    # slow_c3.append(float(lines[9]))
    # slow_c4.append(float(lines[10]))
    _buffer.close()

# policies = ('FCFS', 'SPT', 'LPT', 'WFP3', 'UNI', 'EDD', 'EASY', 'F1', 'F2', 'F3', 'F4')
policies = ('FCFS', 'SPT', 'LPT', 'WFP3', 'UNI')
y_pos = np.arange(len(policies))

performance = []
performance.append(np.mean(slow_fcfs))
performance.append(np.mean(slow_spt))
performance.append(np.mean(slow_lpt))
performance.append(np.mean(slow_wfp3))
performance.append(np.mean(slow_unicef))
# performance.append(np.mean(slow_edd))
# performance.append(np.mean(slow_easy))
# performance.append(np.mean(slow_c1))
# performance.append(np.mean(slow_c2))
# performance.append(np.mean(slow_c3))
# performance.append(np.mean(slow_c4))

error = []
error.append(np.std(slow_fcfs))
error.append(np.std(slow_spt))
error.append(np.std(slow_lpt))
error.append(np.std(slow_wfp3))
error.append(np.std(slow_unicef))
# error.append(np.std(slow_edd))
# error.append(np.std(slow_easy))
# error.append(np.std(slow_c1))
# error.append(np.std(slow_c2))
# error.append(np.std(slow_c3))
# error.append(np.std(slow_c4))

# arrange data
all_data = []
all_data.append(slow_fcfs)
all_data.append(slow_spt)
all_data.append(slow_lpt)
all_data.append(slow_wfp3)
all_data.append(slow_unicef)
# all_data.append(slow_edd)
# all_data.append(slow_easy)
# all_data.append(slow_c1)
# all_data.append(slow_c2)
# all_data.append(slow_c3)
# all_data.append(slow_c4)

plt.rc("font", size=45)
plt.figure(figsize=(16, 14))

# store medians for the boxplot
all_medians = []

axes = plt.axes()

# convert the data format
new_all_data = np.array(all_data)

# plot the outliers for algorithms which the value is out of range
OUT_POLICIES = 5    # change the value to fit the range of algorithms
MAX_OUT = [0, 0, 0, 0, 0]
outliers = np.zeros((OUT_POLICIES, max(MAX_OUT)))
xticks = [y+1 for y in range(len(all_data))]
for i in range(0, OUT_POLICIES):
    temp = new_all_data[i, :]
    plt.plot(xticks[i], np.reshape(temp, (1, len(temp))), 'o', color='darkorange')

# for i in range(0, OUT_POLICIES):
#     temp = new_all_data[i, :]
#     for j in range(0, MAX_OUT[i]):
#         _max = np.max(temp)
#         outliers[i, j] = _max
#         temp = np.delete(temp, np.argmax(temp))

plt.plot(xticks[0:1], new_all_data[0:1], 'o', color='darkorange')
plt.plot(xticks[1:2], new_all_data[1:2], 'o', color='darkorange')
plt.plot(xticks[2:3], new_all_data[2:3], 'o', color='darkorange')
plt.plot(xticks[3:4], new_all_data[3:4], 'o', color='darkorange')
plt.plot(xticks[4:5], new_all_data[4:5], 'o', color='darkorange')
#plt.plot(xticks[5:6], new_all_data[5:6], 'o', color='darkorange')
#plt.plot(xticks[6:7], new_all_data[6:7], 'o', color='darkorange')
#plt.plot(xticks[7:8], new_all_data[7:8], 'o', color='darkorange')

plt.ylim((0, 600))

xoffset = [0.32, 0.32, 0.32, 0.32, 0.32]
ylow = [550, 550, 550, 550, 550]
for i in range(0, OUT_POLICIES):
    output = ''
    if MAX_OUT[i] != 0:
        for j in range(0, MAX_OUT[i]):
            if j == MAX_OUT[i] - 1:
                output = output+'%.1f' % (outliers[i, j])
            else:
                output = output+'%.1f' % (outliers[i, j])+'\n'
        # plot the points
        plt.annotate(output, xy=(xticks[i], 600), xytext=(xticks[i]-xoffset[i], ylow[i]),
                     arrowprops=dict(facecolor='black', shrink=0.05), fontsize=25)

# calculate medians
for p in all_data:
    all_medians.append(np.median(p))

# plot box plot
plt.boxplot(all_data, showfliers=False)

# adding horizontal grid lines
axes.yaxis.grid(True)
axes.set_xticks([y+1 for y in range(len(all_data))])

# add x-tick labels
xticklabels = ['FCFS', 'SPT', 'LPT', 'WFP3', 'UNI']
plt.setp(axes, xticks=[y+1 for y in range(len(all_data))],
            xticklabels=['FCFS', 'SPT', 'LPT', 'WFP3', 'UNI'])
plt.tick_params(axis='both', which='major', labelsize=45)
plt.tick_params(axis='both', which='minor', labelsize=45)

plt.savefig('plots/sched_perf_on_curie_runtime.pdf', format='pdf',
            dpi=1000, bbox_inches='tight')

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
print('Boxplot saved in file plots/sched_perf_on_curie_runtime.pdf')
