/* This program is free software; you can redistribute it and/or modify it
 * under the terms of the license (GNU LGPL) which comes with this package. */

#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <limits.h>
#include <string.h>
#include <stdbool.h>
#include "simgrid/msg.h" /* Yeah! If you want to use msg, you need to include msg/msg.h */
#include "xbt/sysdep.h"  /* calloc, printf */

/* Create a log channel to have nice outputs. */
#include "xbt/log.h"
#include "xbt/asserts.h"

XBT_LOG_NEW_DEFAULT_CATEGORY(msg_test, "Messages specific for this msg example");

/* backfilling mode for the scheduler */
void backFill(double *runtimes, int *cores, int *submit, int *orig_pos, int policy, int queue_num_tasks, int num_tasks_disp);
/* sort jobs in a queue and scheduling them - add one more param - mics */
void sortTasksQueue(double *runtimes, int *cores, int *mics, int *submit, int *duedate, int *orig_pos, int policy, int queue_num_tasks, int num_tasks_disp);
/* get field when reading file */
const char *getfield(char *line, int num);
/* read workload files */
void readModelFile(void);
/* master function in the simgrid simulation */
int master(int argc, char *argv[]);
/* taskManager function in the simgrid simulation */
int taskManager(int argc, char *argv[]);
/* taskMonitor function */
int taskMonitor(int argc, char *argv[]);
/* do the simulationg task for submitting jobs */
msg_error_t test_all(const char *platform_file, const char *application_file);
/* define inv function in math */
double inv(int x);

#define FINALIZE ((void *)221297) /* a magic number to tell people to stop working */

#define MAX_TASKS 1024
#define WORKERS_PER_NODE 1
#define MAX_TASK_TYPES 5
#define TERA 1e12
#define MEGA 1e6
#define TAO 10
#define QUEUE_NUM_TASKS 32
#define NUM_TASKS_STATE 16
#define EPSILON 1e-20

#define FCFS 0
#define SPT 1
#define LPT 2
#define WFP3 3
#define UNICEF 4
#define EDD 5
#define EASY 6
#define F1 7
#define F2 8
#define F3 9
#define F4 10

int number_of_tasks = 0;

/* struct for each job/task in the simulator */
struct task_t
{
	int numNodes;
	int numMICs;
	double startTime;
	double endTime;
	double submitTime;
	double duedate;
	double *task_comp_size;
	double *task_comm_size;
	msg_host_t *task_workers;
	int *task_allocation;
	int *mic_allocation;
};

/* for debug */
int VERBOSE = 0;
int STATE = 0;

/* arrays to store a list of jobs */
double *all_runtimes;
int *all_submit;
int *all_cores;
int *all_mic;
int *all_duedate;

/* tracking task positions */
int *orig_task_positions;

/* metrics for evaluating performance */
double *slowdown;
double *throughput;
double *lateness;

/* array to store a list of queued tasks */
struct task_t *task_queue = NULL;
/* the master process in simgrid */
msg_process_t p_master;

/* the default scheduler for simulation */
int chosen_policy = FCFS;
int *busy_workers;	// for cpu nodes
int *mic_workers;	// for mic nodes
int num_managers;
int number_of_nodes;

double *sched_task_placement;
double t0 = 0.0f;

/* Main function */
int main(int argc, char *argv[])
{
	msg_error_t res = MSG_OK;
	int i;

	MSG_init(&argc, argv);
	if (argc < 3)
	{
		printf("Usage: %s platform_file deployment_file [-verbose]\n", argv[0]);
		printf("example: %s msg_platform.xml msg_deployment.xml -verbose\n", argv[0]);
		exit(1);
	}

	if (argc >= 4)
	{
		for (i = 3; i < argc; i++)
		{
			if (strcmp(argv[i], "-verbose") == 0){
				VERBOSE = 1;
			}
			if (strcmp(argv[i], "-state") == 0){
				STATE = 1;
			}
			if (strcmp(argv[i], "-spt") == 0){
				chosen_policy = SPT;
				printf("SPT\n");
			}
			if (strcmp(argv[i], "-lpt") == 0){
				chosen_policy = LPT;
				printf("LPT\n");
			}
			if (strcmp(argv[i], "-wfp3") == 0){
				chosen_policy = WFP3;
				printf("WFP3\n");
			}
			if (strcmp(argv[i], "-unicef") == 0){
				chosen_policy = UNICEF;
				printf("UNICEF\n");
			}
			if (strcmp(argv[i], "-edd") == 0){
				chosen_policy = EDD;
				printf("EDD\n");
			}
			if (strcmp(argv[i], "-easy") == 0){
				chosen_policy = EASY;
				printf("EASY\n");
			}
			if (strcmp(argv[i], "-f1") == 0){
				chosen_policy = F1;
				printf("F1\n");
			}
			if (strcmp(argv[i], "-f2") == 0){
				chosen_policy = F2;
				printf("F2\n");
			}
			if (strcmp(argv[i], "-f3") == 0){
				chosen_policy = F3;
				printf("F3\n");
			}
			if (strcmp(argv[i], "-f4") == 0){
				chosen_policy = F4;
				printf("F4\n");
			}
			if (strcmp(argv[i], "-nj") == 0)
			{
				number_of_tasks = atoi(argv[i + 1]);
				num_managers = number_of_tasks;
			}
		}
	}
	if (number_of_tasks == 0){
		printf("Invalid number_of_tasks parameter. Please set -nj parameter in runtime.\n");
		exit(1);
	}

	if (chosen_policy == 0)
		printf("FCFS\n");

	res = test_all(argv[1], argv[2]);

	if (res == MSG_OK)
		return 0;
	else
		return 1;
} /* end_of_main */

/* call the simgrid module */
msg_error_t test_all(const char *platform_file, const char *application_file)
{
	msg_error_t res = MSG_OK;
	int i;

	{ /*  Simulation setting */
		MSG_config("host/model", "default");
		MSG_create_environment(platform_file);
	}

	{ /* Application deployment */
		MSG_function_register("master", master);
		MSG_function_register("taskManager", taskManager);
		MSG_function_register("taskMonitor", taskMonitor);

		MSG_launch_application(application_file);

		char sprintf_buffer[64];
		for (i = 0; i < num_managers; i++)
		{
			sprintf(sprintf_buffer, "node-%d", i + 1);
			MSG_process_create("taskManager", taskManager, NULL, MSG_get_host_by_name(sprintf_buffer));
		}

		/* moniroting task */
		sprintf(sprintf_buffer, "node-%d", number_of_tasks + 1);
		MSG_process_create("taskMonitor", taskMonitor, NULL, MSG_get_host_by_name(sprintf_buffer));
	}
	res = MSG_main();

	/* calculate the slowdown metric */
	double sumSlowdown = 0.0f;
	slowdown = (double *)calloc(number_of_tasks - NUM_TASKS_STATE, sizeof(double));
	int _count = 0;
	for (i = NUM_TASKS_STATE; i < number_of_tasks; i++)
	{
		double waitTime = task_queue[i].startTime - task_queue[i].submitTime;
		double runTime = task_queue[i].endTime - task_queue[i].startTime;
		double quocient = runTime >= TAO ? runTime : TAO;
		double slow = (waitTime + runTime) / quocient;
		slowdown[_count] = slow >= 1.0f ? slow : 1.0f;
		sumSlowdown += slowdown[_count];

		if (VERBOSE)
			XBT_INFO("Execution Stats for \"Task_%d\" [r=%.1f,c=%d,m=%d, s=%d]: Wait Time=%.2f, Slowdown=%.2f, Simulated Runtime=%.2f.", orig_task_positions[i], all_runtimes[i], all_cores[i], all_mic[i], all_submit[i], waitTime, slowdown[_count], runTime);
		_count++;
	}

	/* calculate the metric */
	double sumLateness = 0.0f;
	double max_endtime = 0.0f;
	lateness = (double *) calloc(number_of_tasks - NUM_TASKS_STATE, sizeof(double));
	for (i = NUM_TASKS_STATE; i < number_of_tasks; i++){
		double duedate = task_queue[i].duedate;
		lateness[i] = task_queue[i].endTime - duedate;
		sumLateness += lateness[i];
		if(task_queue[i].endTime > max_endtime)
			max_endtime = task_queue[i].endTime;
		
		if (VERBOSE)
			XBT_INFO("Lateness Stats for \"Task_%d\" [r=%.1f,c=%d,m=%d,s=%d]: Duedate=%d, End Time=%.2f.", orig_task_positions[i], all_runtimes[i], all_cores[i], all_mic[i], all_submit[i], all_duedate[i], task_queue[i].endTime);
	}

	double AVGSlowdown = sumSlowdown / (number_of_tasks - NUM_TASKS_STATE);
	double AVGLateness = sumLateness / (number_of_tasks - NUM_TASKS_STATE);
	double Throughput = number_of_tasks * 3600 / max_endtime;

	if (VERBOSE){
		XBT_INFO("Average bounded slowdown & Lateness & Throughput: %f, %f, %f", AVGSlowdown, AVGLateness, Throughput);
		XBT_INFO("Simulation time %g", MSG_get_clock());
	}
	else if (!STATE){
		// printf("%f\n", AVGSlowdown);
		// printf("%f\n", AVGLateness);
		printf("%f\n", Throughput);
	}

	return res;
} /* end test_all() function */

/* the master module for simgrid  */
int master(int argc, char *argv[])
{
	int workers_count = 0;
	number_of_nodes = 0;
	msg_host_t *workers = NULL;
	msg_host_t task_manager = NULL;
	msg_task_t *todo = NULL;

	int i;

	/* get worker_count and number_of_nodes */
	int res = sscanf(argv[1], "%d", &workers_count);
	xbt_assert(res, "Invalid argument %s\n", argv[1]);
	res = sscanf(argv[2], "%d", &number_of_nodes);
	xbt_assert(res, "Invalid argument %s\n", argv[2]);

	/* read the workload file */
	readModelFile();

	/* get the original positions of jobs */
	orig_task_positions = (int *)malloc((number_of_tasks) * sizeof(int));
	int c = 0;
	for (i = 0; i < number_of_tasks; i++){
		orig_task_positions[c++] = i;
	}

	/* Process organisation (workers) - master process */
	p_master = MSG_process_self();
	{
		char sprintf_buffer[64];
		workers = xbt_new0(msg_host_t, workers_count);

		for (i = 0; i < workers_count; i++){
			sprintf(sprintf_buffer, "node-%d", (i + WORKERS_PER_NODE) / WORKERS_PER_NODE);
			workers[i] = MSG_get_host_by_name(sprintf_buffer);
			xbt_assert(workers[i] != NULL, "Unknown host %s. Stopping Now! ", sprintf_buffer);
		}
	}

	/* Process organisation (managers) - prepare task_manager */
	{
		task_manager = MSG_get_host_by_name("node-0");
		xbt_assert(task_manager != NULL, "Unknown host %s. Stopping Now! ", "node-0");
	}

	if (VERBOSE)
		XBT_INFO("Got %d cores and %d tasks to process", number_of_nodes, number_of_tasks);

	/* create task/job submission */
	{
		int j, k;
		todo = xbt_new0(msg_task_t, number_of_tasks);
		busy_workers = (int *)calloc(number_of_nodes, sizeof(int));	// allocate mem for cpu nodes
		mic_workers = (int *)calloc(number_of_nodes, sizeof(int));	// allocate mem for mic nodes
		task_queue = (struct task_t *)malloc(number_of_tasks * sizeof(struct task_t));
		//tasks_comp_sizes = (double**) malloc(number_of_tasks * sizeof(double*));
		//tasks_comm_sizes = (double**) malloc(number_of_tasks * sizeof(double*));
		//tasks_allocation = (int**) malloc(number_of_tasks * sizeof(int*));

		// flag for checking resource enough or not?
		bool is_resource_enough = false;

		for (i = 0; i < number_of_tasks; i++){
			int available_nodes;
			int available_mics;
			do{
				/* init is_resource_enough = false */
				is_resource_enough = false;

				if (i >= NUM_TASKS_STATE){
					sortTasksQueue(&all_runtimes[i], &all_cores[i], &all_mic[i], &all_submit[i], &all_duedate[i], &orig_task_positions[i], chosen_policy, number_of_tasks - i >= QUEUE_NUM_TASKS ? QUEUE_NUM_TASKS : number_of_tasks - i, i);
				}
				/* process jobs that have not arrived yet */
				while (MSG_get_clock() < all_submit[i]){
					MSG_process_sleep(all_submit[i] - MSG_get_clock());
				}

				/* print the detail of job[i] */
				// printf("Job[%d]: runtimes = %f, cores = %d, mics = %d, submit_time = %d\n", i, all_runtimes[i], all_cores[i], all_mic[i], all_submit[i]);
				
				/* check available nodes for job[i] */
				available_nodes = 0;
				for (j = 0; j < number_of_nodes; j++){
					if (busy_workers[j] == 0){
						available_nodes++;
						if (available_nodes == all_cores[i]){
							break;
						}
					}
				}

				/* check available mics for job[i] */
				available_mics = 0;
				if (all_mic[i] > 0){
					for (j = 0; j < number_of_nodes; j++){
						if(mic_workers[j] == 0){
							available_mics++;
							if (available_mics == all_mic[i]){
								break;
							}
						}
					}
					if (available_nodes == all_cores[i] && available_mics == all_mic[i])
						is_resource_enough = true;
				}else {
					if (available_nodes == all_cores[i])
						is_resource_enough = true;
				}

				/* if there is no available nodes, suspend job[i] */
				if (is_resource_enough == false){
					if (VERBOSE)
						XBT_INFO("Insuficient workers for \"Task_%d\" [r=%.1f,c=%d,m=%d,s=%d] (%d available nodes - %d available mics. need %d). Waiting.", orig_task_positions[i], all_runtimes[i], all_cores[i], all_mic[i], all_submit[i], available_nodes, available_mics, all_cores[i]);
					MSG_process_suspend(p_master);
				}
			} while (is_resource_enough == false);

			/* assign information to job[i] if it is processed */
			task_queue[i].numNodes = all_cores[i];
			task_queue[i].numMICs = all_mic[i];
			task_queue[i].startTime = 0.0f;
			task_queue[i].endTime = 0.0f;
			task_queue[i].submitTime = all_submit[i];
			task_queue[i].duedate = all_duedate[i];	// for duedate-values
			task_queue[i].task_allocation = (int *)malloc((all_cores[i]) * sizeof(int));
			task_queue[i].mic_allocation = (int *)malloc((all_mic[i]) * sizeof(int));
			//task_queue[i].task_comp_size = (double*) malloc(all_cores[i] * sizeof(double));
			//task_queue[i].task_comm_size = (double*) malloc(all_cores[i] * all_cores[i] * sizeof(double));
			//task_queue[i].task_workers = (msg_host_t*) calloc(all_cores[i], sizeof(msg_host_t));

			/* update mic workers */
			int count = 0;
			if (all_mic[i] != 0){
				for(j = 0; j < number_of_nodes; j++){
					if (mic_workers[j] == 0){
						task_queue[i].mic_allocation[count] = j;
						mic_workers[j] = 1;
						count++;
					}
					if (count >= all_mic[i]){
						break;
					}
				}
			}
			/* update busy workers */
			count = 0;
			for (j = 0; j < number_of_nodes; j++){
				if (busy_workers[j] == 0){
					task_queue[i].task_allocation[count] = j;
					busy_workers[j] = 1;
					count++;
				}
				if (count >= all_cores[i]){
					break;
				}
			}

			msg_host_t self = MSG_host_self();
			double speed = MSG_host_get_speed(self);

			double comp_size = all_runtimes[i] * speed;
			double comm_size = 1000.0f; //0.001f * MEGA;

			char sprintf_buffer[64];
			if (i < NUM_TASKS_STATE){
				sprintf(sprintf_buffer, "Job_%d", i);
			}
			else{
				sprintf(sprintf_buffer, "Job_%d", orig_task_positions[i]);
			}
			
			todo[i] = MSG_task_create(sprintf_buffer, comp_size, comm_size, &task_queue[i]);

			if (VERBOSE)
				XBT_INFO("Dispatching \"%s\" [r=%.1f,c=%d,m=%d,s=%d]", todo[i]->name, all_runtimes[i], all_cores[i], all_mic[i], all_submit[i]);
			MSG_task_send(todo[i], MSG_host_get_name(workers[i]));

			/* update runtime for jobs */
			if (i == NUM_TASKS_STATE - 1){
				t0 = MSG_get_clock();

				if (STATE){
					float *elapsed_times = (float *)calloc(number_of_nodes, sizeof(float));
					for (j = 0; j <= i; j++){
						if (task_queue[j].endTime == 0.0f){
							float task_elapsed_time = MSG_get_clock() - task_queue[j].startTime;
							if (j == i){
								task_elapsed_time = 0.01f;
							}
							for (k = 0; k < all_cores[j]; k++){
								elapsed_times[(task_queue[j].task_allocation[k])] = task_elapsed_time;
							}
						}
					}
					for (j = 0; j < number_of_nodes; j++){
						if (j < number_of_nodes - 1)
							printf("%f,", elapsed_times[j]);
						else
							printf("%f\n", elapsed_times[j]);
					}
					break;
				}
			}
		}

		if (STATE){
			if (VERBOSE)
				XBT_INFO("All tasks have been dispatched. Let's tell everybody the computation is over.");
			for (i = NUM_TASKS_STATE; i < num_managers; i++){
				msg_task_t finalize = MSG_task_create("finalize", 0, 0, FINALIZE);
				MSG_task_send(finalize, MSG_host_get_name(workers[i]));
			}
		}

		if (VERBOSE)
			XBT_INFO("Goodbye now!");
			
		free(workers);
		free(todo);
		return 0;
	}
} /* end of master */

/* the slave module for simgrid - receiver function - task manager  */
int taskManager(int argc, char *argv[]){
	msg_task_t task = NULL;
	struct task_t *_task = NULL;
	int i;
	int res;
	
	res = MSG_task_receive(&(task), MSG_host_get_name(MSG_host_self()));
	xbt_assert(res == MSG_OK, "MSG_task_receive failed");
	_task = (struct task_t *)MSG_task_get_data(task);

	if (VERBOSE)
		XBT_INFO("Received \"%s\"", MSG_task_get_name(task));

	if (!strcmp(MSG_task_get_name(task), "finalize")){
		MSG_task_destroy(task);
		return 0;
	}

	if (VERBOSE)
		XBT_INFO("Processing \"%s\"", MSG_task_get_name(task));

	/* get start time & end time */
	_task->startTime = MSG_get_clock();
	MSG_task_execute(task);
	_task->endTime = MSG_get_clock();

	if (VERBOSE)
		XBT_INFO("\"%s\" done", MSG_task_get_name(task));

	/* free busy workers when jobs are free */
	int *allocation = _task->task_allocation;	// track positions of cpu nodes
	int *mic_alloc = _task->mic_allocation;		// track positions of mic nodes
	int n = _task->numNodes;
	int m = _task->numMICs;
	/* free busy_workers when the job finished */
	for (i = 0; i < n; i++){
		busy_workers[allocation[i]] = 0;
	}
	/* free mic_workers when the job finished */
	if (m != 0){
		for (i = 0; i < m; i++){
			mic_workers[mic_alloc[i]] = 0;
		}
	}
	/* destroy the job */
	MSG_task_destroy(task);
	task = NULL;
	MSG_process_resume(p_master);
	
	return 0;
} /* end_of_worker */


/* Sort Task Queue */
void sortTasksQueue(double *runtimes, int *cores, int *mics, int *submit, int *duedate, int *orig_pos, int policy, int queue_num_tasks, int num_tasks_disp){
	int i, j;
	int curr_time = MSG_get_clock();	// current time
	int num_arrived_tasks = 0;

	/* count the arrived jobs at the current time */
	for (i = 0; i < queue_num_tasks; i++){
		if (submit[i] <= curr_time){
			num_arrived_tasks++;
		}
		else{
			break;
		}
	}
	/* check the number of arrived jobs */
	if (num_arrived_tasks == 1)
		return;

	/* scheduling algorithms */
	/* num_tasks_disp: ??? */
	if (policy == EASY){
		/* estimate the remaining time for the job is processed */
		double *remaining_time = (double *)calloc(num_tasks_disp, sizeof(double));
		for (i = 0; i < num_tasks_disp; i++){
			if (task_queue[i].endTime == 0.0f){
				float task_elapsed_time = curr_time - task_queue[i].startTime;
				remaining_time[i] = all_runtimes[i] - task_elapsed_time;
			}
			else{
				remaining_time[i] = -1.0;
			}
		}

		/* check available nodes for job[i] */
		int available_nodes = 0;
		for (j = 0; j < number_of_nodes; j++){
			if (busy_workers[j] == 0){
				available_nodes++;
			}
		}
		/* check ??? */
		int shadow_time = 0;
		int available_nodes_future = 0;
		int extra_nodes = 0;
		int min_remaining = INT_MAX;
		int min_remaining_task = 0;
		if (available_nodes < cores[0]){
			for (i = 0; i < num_tasks_disp; i++){
				min_remaining = INT_MAX;
				for (j = 0; j < num_tasks_disp; j++){
					if (remaining_time[j] != -1.0 && remaining_time[j] < min_remaining){
						min_remaining = remaining_time[j];
						min_remaining_task = j;
					}
				}
				remaining_time[min_remaining_task] = INT_MAX;
				available_nodes_future += all_cores[min_remaining_task];
				if (available_nodes + available_nodes_future >= cores[0]){
					shadow_time = curr_time + min_remaining;
					extra_nodes = (available_nodes + available_nodes_future) - cores[0];
					break;
				}
			}
		}

		for (i = 1; i < num_arrived_tasks; i++){
			if ((cores[i] <= available_nodes && (curr_time + runtimes[i]) <= shadow_time) || (cores[i] <= (available_nodes < extra_nodes ? available_nodes : extra_nodes))){
				if (VERBOSE)
					XBT_INFO("\"Task_%d\" [r=%.1f,c=%d, s=%d] Backfilled. Shadow Time=%d, Extra Nodes=%d.", orig_pos[i], runtimes[i], cores[i], submit[i], shadow_time, extra_nodes);
				
				double r_buffer = runtimes[i];
				int c_buffer = cores[i];	// for core_buffer
				int m_buffer = mics[i];		// for mic_buffer
				int s_buffer = submit[i];
				int d_buffer = duedate[i];	// for duedate_buffer
				int p_buffer = orig_pos[i];
				for (j = i; j > 0; j--){
					runtimes[j] = runtimes[j - 1];
					cores[j] = cores[j - 1];
					mics[j] = mics[j - 1];
					submit[j] = submit[j - 1];
					duedate[j] = duedate[j - 1];	// for duedate_buffer
					orig_pos[j] = orig_pos[j - 1];
				}
				
				runtimes[0] = r_buffer;
				cores[0] = c_buffer;
				mics[0] = m_buffer;
				submit[0] = s_buffer;
				duedate[0] = d_buffer;	// for duedate_buffer
				orig_pos[0] = p_buffer;
				break;
			}
		}
		free(remaining_time);
		return;
	}

	/* FCFS */
	if (policy == FCFS){
		return;
	}

	/* SPT policy */
	if (policy == SPT){
		double r_buffer;	//runtimes
		int c_buffer;	// cores
		int m_buffer;	// mics
		int s_buffer;	// submit time
		int d_buffer;	// duedate
		int p_buffer;	// position buffer
		for (i = 0; i < num_arrived_tasks; i++){
			for (j = 0; j < num_arrived_tasks; j++)
			{
				if (runtimes[i] < runtimes[j]){
					r_buffer = runtimes[i];
					c_buffer = cores[i];
					m_buffer = mics[i];
					s_buffer = submit[i];
					d_buffer = duedate[i];
					p_buffer = orig_pos[i];

					runtimes[i] = runtimes[j];
					cores[i] = cores[j];
					mics[i] = mics[j];
					submit[i] = submit[j];
					duedate[i] = duedate[j];
					orig_pos[i] = orig_pos[j];

					runtimes[j] = r_buffer;
					cores[j] = c_buffer;
					mics[j] = m_buffer;
					submit[j] = s_buffer;
					duedate[j] = d_buffer;
					orig_pos[j] = p_buffer;
				}
			}
		}
		return;
	}

	/* LPT policy */
    if(policy == LPT){
        double r_buffer;    // runtimes
        int c_buffer;   // cores
		int m_buffer;	// mics
        int s_buffer;   // submit time
		int d_buffer;	// duedate
        int p_buffer;   // position
        for(i = 0; i < num_arrived_tasks; i++){
            for(j = 0; j < num_arrived_tasks; j++){
                if (runtimes[i] > runtimes[j]){
                    r_buffer = runtimes[i];
                    c_buffer = cores[i];
					m_buffer = mics[i];
                    s_buffer = submit[i];
					d_buffer = duedate[i];
                    p_buffer = orig_pos[i];

                    runtimes[i] = runtimes[j];
                    cores[i] = cores[j];
					mics[i] = mics[j];
                    submit[i] = submit[j];
					duedate[i] = duedate[j];
					orig_pos[i] = orig_pos[j];

                    runtimes[j] = r_buffer;
                    cores[j] = c_buffer;
					mics[j] = m_buffer;
                    submit[j] = s_buffer;
					duedate[j] = d_buffer;
					orig_pos[j] = p_buffer;
                }
            }
        }
        return;
    }

	/* EDD policy */
    if(policy == EDD){
        double r_buffer;    // runtimes
        int c_buffer;   // core
		int m_buffer;
        int s_buffer;   // submit
		int d_buffer;	// duedate
        int p_buffer;   // position
        for(i = 0; i < num_arrived_tasks; i++){
            for(j = 0; j < num_arrived_tasks; j++){
                if (duedate[i] < duedate[j]){
                    r_buffer = runtimes[i];
                    c_buffer = cores[i];
					m_buffer = mics[i];
                    s_buffer = submit[i];
					d_buffer = duedate[i];
					p_buffer = orig_pos[i];

                    runtimes[i] = runtimes[j];
                    cores[i] = cores[j];
					mics[i] = mics[j];
                    submit[i] = submit[j];
					duedate[i] = duedate[j];
					orig_pos[i] = orig_pos[j];

                    runtimes[j] = r_buffer;
                    cores[j] = c_buffer;
					mics[j] = m_buffer;
                    submit[j] = s_buffer;
					duedate[j] = d_buffer;
                    orig_pos[j] = p_buffer;
                }
            }
        }
        return;
    }

	/* calculate the priority of jobs for other algorithms */
	double *h_values = (double *)calloc(num_arrived_tasks, sizeof(double));
	double *r_temp = (double *)calloc(num_arrived_tasks, sizeof(double));
	int *c_temp = (int *)calloc(num_arrived_tasks, sizeof(int));
	int *m_temp = (int *)calloc(num_arrived_tasks, sizeof(int));	// mics_temp
	int *s_temp = (int *)calloc(num_arrived_tasks, sizeof(int));
	int *d_temp = (int *)calloc(num_arrived_tasks, sizeof(int));	// duedate_temp
	int *p_temp = (int *)calloc(num_arrived_tasks, sizeof(int));

	/* find the job which has the latest submit time */
	int max_arrive = 0;
	for (i = 0; i < num_arrived_tasks; i++){
		if (submit[i] > max_arrive){
			max_arrive = submit[i];
		}
	}

	/* scheduling functions */
	int task_age = 0;
	for (i = 0; i < num_arrived_tasks; i++){
		task_age = curr_time - submit[i];
		switch (policy){
		case 30:
			h_values[i] = ((float)task_age / (float)runtimes[i]) * cores[i];
			break;
		case WFP3:
			h_values[i] = pow((float)task_age / (float)runtimes[i], 3) * cores[i];
			break;
		case UNICEF:
			h_values[i] = (task_age + EPSILON) / (log2((double)cores[i] + EPSILON) * runtimes[i]);
			break;
		case F1:
			// h_values[i] = (0.0056500287 * runtimes[i]) * (0.0000024814 * sqrt(cores[i])) + (0.0074444355 * log10(submit[i])); //256nodes
			h_values[i] = duedate[i] * (-0.0000000314 * runtimes[i] * (-0.0036668288 * cores[i])) + (0.0103053379 * log10(submit[i]));
			break;
		case F2:
			// h_values[i] = (-0.2188093701 * runtimes[i]) * (-0.0000000049 * cores[i]) + (0.0073580361 * log10(submit[i])); //256nodes
			h_values[i] = 49.5155372 * sqrt(runtimes[i]) / (129.3129007 * (inv(cores[i]))) + 219.4333513 * log10(submit[i]) / (7004.1400155 * log10(duedate[i]) + mics[i]);
			break;
		case F3:
			// h_values[i] = (0.0000342717 * sqrt(runtimes[i])) * (0.0076562110 * cores[i]) + (0.0067364626 * log10(submit[i])); //256nodes
			h_values[i] = 6.9867922 * runtimes[i] / (375.3895129 * (inv(cores[i]))) + 429.4184138 * log10(submit[i]) / (13121.1874123 * log10(duedate[i]) + mics[i]);
			break;
		case F4:
			h_values[i] = (-0.0155183403 * log10(runtimes[i])) * (-0.0005149209 * cores[i]) + (0.0069596182 * log10(submit[i])); //256nodes
			break;
		}
		if (VERBOSE)
			XBT_INFO("Score for \"Task_%d\" [r=%.1f,c=%d,m=%d,s=%d,d=%d=%.7f]", orig_pos[i], runtimes[i], cores[i], mics[i], submit[i], duedate[i], h_values[i]);
	}

	/* for WFP3 & UNICEF */
	if (policy == WFP3 || policy == UNICEF){
		double max_val = 0.0;
		int max_index = 0;
		for (i = 0; i < num_arrived_tasks; i++){
			max_val = -1e20;
			for (j = 0; j < num_arrived_tasks; j++){
				if (h_values[j] > max_val){
					max_val = h_values[j];
					max_index = j;
				}
			}
			r_temp[i] = runtimes[max_index];
			c_temp[i] = cores[max_index];
			m_temp[i] = mics[max_index];
			s_temp[i] = submit[max_index];
			d_temp[i] = duedate[max_index];
			p_temp[i] = orig_pos[max_index];
			h_values[max_index] = -1e20;
		}
	}
	else if (policy >= F1){
		double min_val = 1e20;
		int min_index;
		for (i = 0; i < num_arrived_tasks; i++){
			min_val = 1e20;
			min_index = 0;
			for (j = 0; j < num_arrived_tasks; j++){
				if (h_values[j] < min_val){
					min_val = h_values[j];
					min_index = j;
				}
			}
			r_temp[i] = runtimes[min_index];
			c_temp[i] = cores[min_index];
			m_temp[i] = mics[min_index];
			s_temp[i] = submit[min_index];
			d_temp[i] = duedate[min_index];
			p_temp[i] = orig_pos[min_index];
			h_values[min_index] = 1e20;
		}
	}
	for (i = 0; i < num_arrived_tasks; i++){
		runtimes[i] = r_temp[i];
		cores[i] = c_temp[i];
		mics[i] = m_temp[i];
		submit[i] = s_temp[i];
		duedate[i] = d_temp[i];
		orig_pos[i] = p_temp[i];
	}

	free(r_temp);
	free(c_temp);
	free(m_temp);
	free(s_temp);
	free(d_temp);
	free(p_temp);
	free(h_values);
}

/* the moniroting function */
int taskMonitor(int argc, char *argv[]){
	int i;
	for (i = 0; i < number_of_tasks; i++){
		while (MSG_get_clock() < all_submit[i]){ // the job has not arrived yet
			MSG_process_sleep(all_submit[i] - MSG_get_clock());
		}
		if (VERBOSE)
			XBT_INFO("\"Task_%d\" [r=%.1f,c=%d,m=%d,s=%d] arrived. Waking up master.", i, all_runtimes[i], all_cores[i], all_mic[i], all_submit[i]);
		MSG_process_resume(p_master);
	}
	return 0;
}

/* the backfilling function */
void backFill(double *runtimes, int *cores, int *submit, int *orig_pos, int policy, int queue_num_tasks, int num_tasks_disp){
	int i, j;
	int curr_time = MSG_get_clock();
	int num_arrived_tasks = 0;
	/* count the number of arrived jobs at the current time */
	for (i = 0; i < queue_num_tasks; i++){
		if (submit[i] <= curr_time){
			num_arrived_tasks++;
		}
		else{
			break;
		}
	}
	
	if (num_arrived_tasks == 1)
		return;

	double *remaining_time = (double *)calloc(num_tasks_disp, sizeof(double));
	for (i = 0; i < num_tasks_disp; i++){
		if (task_queue[i].endTime == 0.0f){
			float task_elapsed_time = curr_time - task_queue[i].startTime;
			remaining_time[i] = all_runtimes[i] - task_elapsed_time;
		}
		else{
			remaining_time[i] = -1.0;
		}
	}

	int available_nodes = 0;
	for (j = 0; j < number_of_nodes; j++){
		if (busy_workers[j] == 0){
			available_nodes++;
		}
	}

	int shadow_time = 0;
	int available_nodes_future = 0;
	int extra_nodes = 0;
	int min_remaining = INT_MAX;
	int min_remaining_task = 0;
	if (available_nodes < cores[0]){
		for (i = 0; i < num_tasks_disp; i++){
			min_remaining = INT_MAX;
			for (j = 0; j < num_tasks_disp; j++){
				if (remaining_time[j] != -1.0 && remaining_time[j] < min_remaining){
					min_remaining = remaining_time[j];
					min_remaining_task = j;
				}
			}
			remaining_time[min_remaining_task] = INT_MAX;
			available_nodes_future += all_cores[min_remaining_task];
			if (available_nodes + available_nodes_future >= cores[0]){
				shadow_time = curr_time + min_remaining;
				extra_nodes = (available_nodes + available_nodes_future) - cores[0];
				break;
			}
		}
	}

	for (i = 1; i < num_arrived_tasks; i++){
		if ((cores[i] <= available_nodes && (curr_time + runtimes[i]) <= shadow_time) || (cores[i] <= (available_nodes < extra_nodes ? available_nodes : extra_nodes))){
			if (VERBOSE)
				XBT_INFO("\"Task_%d\" [r=%.1f,c=%d, s=%d] Backfilled. Shadow Time=%d, Extra Nodes=%d.", orig_pos[i], runtimes[i], cores[i], submit[i], shadow_time, extra_nodes);
			double r_buffer = runtimes[i];
			int c_buffer = cores[i];
			int s_buffer = submit[i];
			int p_buffer = orig_pos[i];
			for (j = i; j > 0; j--){
				runtimes[j] = runtimes[j - 1];
				cores[j] = cores[j - 1];
				submit[j] = submit[j - 1];
				orig_pos[j] = orig_pos[j - 1];
			}
			runtimes[0] = r_buffer;
			cores[0] = c_buffer;
			submit[0] = s_buffer;
			orig_pos[0] = p_buffer;
			break;
		}
	}
}

/* getfield for reading workload-files */
const char *getfield(char *line, int num){
	const char *tok;
	for (tok = strtok(line, ","); tok && *tok; tok = strtok(NULL, ",\n")){
		if (!--num)
			return tok;
	}
	return NULL;
}

/* Define inv function */
double inv(int x){
    double result = 1.0 / (x + 1e-10);
    return result;
}

/* read workload files */
void readModelFile(void){

	all_runtimes = (double *)malloc((number_of_tasks) * sizeof(double));
	all_submit = (int *)malloc((number_of_tasks) * sizeof(int));
	all_cores = (int *)malloc((number_of_tasks) * sizeof(int));
	all_mic = (int *)malloc((number_of_tasks) * sizeof(int));
	all_duedate = (int *)malloc((number_of_tasks) * sizeof(int));
	int task_count = 0;

	FILE *stream = fopen("initial-simulation-submit.csv", "r");

	char line[1024];
	while (fgets(line, 1024, stream)){
		char *tmp = strdup(line);
		all_runtimes[task_count] = atof(getfield(tmp, 1));
		free(tmp);

		tmp = strdup(line);
		all_cores[task_count] = atoi(getfield(tmp, 2));
		free(tmp);

		tmp = strdup(line);
		all_mic[task_count] = atoi(getfield(tmp, 3));
		free(tmp);

		tmp = strdup(line);
		all_submit[task_count] = atoi(getfield(tmp, 4));
		free(tmp);

		tmp = strdup(line);
		all_duedate[task_count] = atoi(getfield(tmp, 5));
		free(tmp);

		// NOTE strtok clobbers tmp
		task_count++;
	}
}
