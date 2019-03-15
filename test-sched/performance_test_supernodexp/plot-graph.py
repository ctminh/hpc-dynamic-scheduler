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

# for slowdown
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

# file input to read
filename = "./plot-data.dat"
# read file
for line in file(filename):
    row = re.split(",", line.strip("\n"))
    slowdown_fcfs.append(float(row[0]))
    slowdown_spt.append(float(row[1]))
    slowdown_lpt.append(float(row[2]))
    slowdown_wfp3.append(float(row[3]))
    slowdown_unicef.append(float(row[4]))
    slowdown_edd.append(float(row[5]))
    slowdown_c1.append(float(row[6]))
    slowdown_c2.append(float(row[7]))
    slowdown_c3.append(float(row[8]))
    slowdown_c4.append(float(row[9]))

performance = []
performance.append(np.mean(slowdown_fcfs))
performance.append(np.mean(slowdown_spt))
performance.append(np.mean(slowdown_lpt))
performance.append(np.mean(slowdown_wfp3))
performance.append(np.mean(slowdown_unicef))
# performance.append(np.mean(slowdown_edd))
performance.append(np.mean(slowdown_c1))
performance.append(np.mean(slowdown_c2))
# performance.append(np.mean(slowdown_c3))
# performance.append(np.mean(slowdown_c4))

error = []
error.append(np.std(slowdown_fcfs))
error.append(np.std(slowdown_spt))
error.append(np.std(slowdown_lpt))
error.append(np.std(slowdown_wfp3))
error.append(np.std(slowdown_unicef))
# error.append(np.std(slowdown_edd))
error.append(np.std(slowdown_c1))
error.append(np.std(slowdown_c2))
# error.append(np.std(slowdown_c3))
# error.append(np.std(slowdown_c4))

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
# all_data.append(slowdown_edd)
all_data.append(slowdown_c1)
all_data.append(slowdown_c2)
# all_data.append(slowdown_c3)
# all_data.append(slowdown_c4)

# medians for the boxplot
all_medians = []

# create axes for the boxplot
axes = plt.axes()

# converting data to the numpy format
np_converted_data = np.array(all_data)

# for drawing arrows which dedicate the out values on the box plot
OUT_POLICIES = 2
MAX_OUT = [1,1]
outliers = np.zeros((OUT_POLICIES,max(MAX_OUT))) # a list: 6x2 [[ 0.  0.], [ 0.   0.], ...]

for i in range(0,OUT_POLICIES):
    temp = np_converted_data[i,:] # get: fcfs, spt (because they are out of values of slowdown)
    for j in range(0,MAX_OUT[i]):
        _max = np.max(temp) # get max: fcfs, spt (run 2-loop)
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
# plt.plot(xticks[5:6], np_converted_data[5:6], 'o', color='darkorange')
plt.plot(xticks[5:6], np_converted_data[5:6], 'o', color='darkorange')
plt.plot(xticks[6:7], np_converted_data[6:7], 'o', color='darkorange')
# plt.plot(xticks[8:9], np_converted_data[8:9], 'o', color='darkorange')
# plt.plot(xticks[9:10], np_converted_data[9:10], 'o', color='darkorange')

# set the y-axis lim
# plt.ylim((40, 100))

# x_offset and y_low ???
x_offset = [0.255, 0.255]
y_low = [92, 92]

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
#     plt.annotate(output, xy=(xticks[i], 97), xytext=(xticks[i]-x_offset[i], y_low[i]),
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
# xticklabels=['FCFS', 'SPT', 'LPT', 'WFP3', 'UNI', 'EDD', 'C1', 'C2', 'C3', 'C4']
xticklabels=['FCFS', 'SPT', 'LPT', 'WFP3', 'UNI', 'C1', 'C2']
# plt.setp(axes, xticks=[y+1 for y in range(len(all_data))], xticklabels=['FCFS', 'SPT', 'LPT', 'WFP3', 'UNI', 'EDD', 'C1', 'C2', 'C3', 'C4'])
plt.setp(axes, xticks=[y+1 for y in range(len(all_data))], xticklabels=['FCFS', 'SPT', 'LPT', 'WFP3', 'UNI', 'C1', 'C2'])

plt.tick_params(axis='both', which='major', labelsize=28)
plt.tick_params(axis='both', which='minor', labelsize=28)

# save image to file - for slowdown
# plt.savefig('plots/slowdown-supernode-xp.pdf', format='pdf', dpi=1000, bbox_inches='tight')
# print('Boxplot saved in file slowdown-supernode-xp.pdf')
# save image to file - for throughput
# plt.savefig('plots/throughput-supernode-xp.pdf', format='pdf', dpi=1000, bbox_inches='tight')
# print('Boxplot saved in file throughput-slowdown-supernode-xp.pdf')
# save image to file - for lateness
plt.savefig('plots/lateness-supernode-xp.pdf', format='pdf', dpi=1000, bbox_inches='tight')
print('Boxplot saved in file lateness-supernode-xp.pdf')

plt.show()