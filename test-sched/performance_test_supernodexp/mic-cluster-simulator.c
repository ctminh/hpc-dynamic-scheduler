/* This program is free software; you can redistribute it and/or modify it
 * under the terms of the license (GNU LGPL) which comes with this package. */

#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <string.h>
#include "simgrid/msg.h"        /* Yeah! If you want to use msg, you need to include msg/msg.h */
#include "xbt/sysdep.h"         /* calloc, printf */
#include <stdbool.h>

/* Create a log channel to have nice outputs. */
#include "xbt/log.h"
#include "xbt/asserts.h"

XBT_LOG_NEW_DEFAULT_CATEGORY(msg_test, "Messages specific for this msg example");

/* sort jobs in a queue and scheduling them */
void sortTasksQueue(double *runtimes, int *cores, int *submit, int *mic, int *duedate, int policy);

const char *getfield(char *line, int num);
/* read the input file of jobs */
void readModelFile(void);
/* master function in the simulator */
int master(int argc, char *argv[]);
/* the function for manager jobs/tasks */
int taskManager(int argc, char *argv[]);
/* assign jobs to execution nodes */
int * assignNode(int *workers_status, int *mic_status, int cores, int mic, int num_workers, int node);
/* look like the main function */
msg_error_t call_simgrid_module(const char *platform_file, const char *application_file);
/* define inv function in math */
double inv(int x);

#define FINALIZE ((void*)221297)        /* a magic number to tell people to stop working */

#define MAX_TASKS 1024
#define WORKERS_PER_NODE 1
#define MAX_TASK_TYPES 5 
#define TERA 1e12
#define MEGA 1e6
#define TAO 10
#define NUM_RUN_TASKS 32    /* num of tasks for running and evaluating */
#define NUM_INIT_TASKS 16   /* num of tasks for warming up/initializing */
#define NUM_COMPUTE_NODES 3 /* num of compute nodes */
#define SPRINTF_BUFFER_SIZE 64

#define FCFS 0
#define LPT 1
#define WFP3 2
#define UNICEF 3
#define CANDIDATE1 4
#define CANDIDATE2 5
#define SPT 6
#define EDD 7
#define SCOUT 8
#define CANDIDATE3 9
#define CANDIDATE4 10

/* struct for each job/task in the simulator */
struct task_t{
    int numNodes;
    int numMICs;
    double startTime;
    double endTime;
    double submitTime;
    double duedate;
    double *task_comp_size;
    double *task_comm_size;
    msg_host_t *task_workers;
    int node_allocation;
    int *task_allocation;
};

/* for debug */
int VERBOSE = 0;
int STATE = 0;

/* arrays to store a list of jobs */
double *model_runtimes;
int *model_submit;
int *model_cores;
int *model_mic;
int *model_duedate;

/* array to store duedate values */
double *list_duedate; 

/* to store the positions of job */
int *orig_task_position;    

/* arrays to store metric values */
double *slowdown;   /* slowdown value of each job */
double *lateness;   /* ateness values */

/* array to store a list of queued tasks */
struct task_t *queue_task = NULL;

/* the master process in simgrid */
msg_process_t p_master;

/* number of tasks for simulating */
int num_managers;
int number_of_tasks;
int number_of_nodes;
// double *sched_task_placement;
double t0 = 0.0f;

/* default policy fo scheduler */
int chosen_policy = FCFS;   

/* to mark busy cores when running jobs */
/* each busy worker represent a compute node */
int *busy_workers[NUM_COMPUTE_NODES];
int *busy_mic; /* mic on nodes */

/* log file */
FILE * logfile;

/* the main function */
int main(int argc, char *argv[]){
    /* message error flag */
    msg_error_t res = MSG_OK;

    int i;

    /* init the simulator */
    MSG_init(&argc, argv);
    if (argc < 3) {
        printf("Usage: %s platform_file deployment_file [-verbose]\n", argv[0]);
        printf("example: %s msg_platform.xml msg_deployment.xml -verbose\n", argv[0]);
        exit(1);
    }

    /* check the parameters */
    if (argc >= 4){
        for(i = 3;i < argc; i++){
            if (strcmp(argv[i], "-verbose") == 0){
                VERBOSE = 1;
            }
            if (strcmp(argv[i], "-state") == 0){
                STATE = 1;
            }
            if (strcmp(argv[i], "-lpt") == 0){
                chosen_policy = LPT;
                printf("LPT\n");
            }
            if (strcmp(argv[i], "-spt") == 0){
                chosen_policy = SPT;
                printf("SPT\n");
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
            if (strcmp(argv[i], "-c1") == 0){
                chosen_policy = CANDIDATE1;
                printf("C1\n");
            }
            if (strcmp(argv[i], "-c2") == 0){
                chosen_policy = CANDIDATE2;
                printf("C2\n");
            }
            if (strcmp(argv[i], "-c3") == 0){
                chosen_policy = CANDIDATE3;
                printf("C3\n");
            }
            if (strcmp(argv[i], "-c4") == 0){
                chosen_policy = CANDIDATE4;
                printf("C4\n");
            }
            if (strcmp(argv[i], "-nj") == 0){
                number_of_tasks = atoi(argv[i+1]);
                num_managers = number_of_tasks;
            }
        }
    }

    if (chosen_policy == 0)
        printf("FCFS\n");

    /* check the number_of_taks == 0 ? */
    if (number_of_tasks == 0){
        printf("Invalid number_of_tasks parameter. Please set -nj parameter in runtime.\n");
        exit(1);
    }

    /* call simgrid module */
    // printf("[Main function] 2. Call the simgrid module\n");
    res = call_simgrid_module(argv[1], argv[2]);

    if (res == MSG_OK){
        return 0;
    }else{
        return 1;
    }
}

/* call the simgrid module */
msg_error_t call_simgrid_module(const char *platform_file, const char *application_file){

    /* message error flag */
    msg_error_t res = MSG_OK;
    int i;

    /* Simulation setting */
    {
        // printf("\t call_simgrid: 2.1. simulation setting\n");
        MSG_config("host/model", "default");
        MSG_create_environment(platform_file);
    }

    /* Application deployment */
    {
        // printf("\t call_simgrid: 2.2. simulation setting\n");
        MSG_function_register("master", master);

        MSG_function_register("taskManager", taskManager);

        MSG_launch_application(application_file);
        
        char sprintf_buffer[SPRINTF_BUFFER_SIZE];
        for(i = 0; i < num_managers; i++){
            sprintf(sprintf_buffer, "node-%d", i + 1);
            MSG_process_create("taskManager", taskManager, NULL, MSG_get_host_by_name(sprintf_buffer));
        }

        /* monitoring tasks */
        // sprintf(sprintf_buffer, "node-%d", number_of_tasks + 1);
        // MSG_process_create("taskMonitor", taskMonitor, NULL, MSG_get_host_by_name(sprintf_buffer));
    }

    // printf("\t call_simgrid: 2.3. running MSG_main()\n");
    res = MSG_main();

    /* calculate the metric - slowdown */
    double sumSlowdown = 0.0f;
    slowdown = (double *) calloc(number_of_tasks - NUM_INIT_TASKS, sizeof(double));
    int _count = 0;
    for(i = NUM_INIT_TASKS; i < number_of_tasks; i++){
        double waitTime = queue_task[i].startTime - queue_task[i].submitTime;
        double runTime = queue_task[i].endTime - queue_task[i].startTime;
        double quocient = runTime >= TAO ? runTime : TAO;
        double slow = (waitTime + runTime) / quocient;
        slowdown[_count] = slow >= 1.0f ? slow : 1.0f;
        sumSlowdown += slowdown[_count];
        _count++;
    }

    /* calculate the metric - lateness */
    double sumLateness = 0.0f;
    double max_endtime = 0.0f;
    lateness = (double *) calloc(number_of_tasks - NUM_INIT_TASKS, sizeof(double));
    list_duedate = (double *) calloc(number_of_tasks - NUM_INIT_TASKS, sizeof(double));
    for(i = NUM_INIT_TASKS; i < number_of_tasks; i++){
        // double runTime = task_queue[i].endTime - task_queue[i].startTime;
        double duedate = queue_task[i].duedate;
        list_duedate[i] = duedate;
        lateness[i] = queue_task[i].endTime -  duedate;
        // printf("endtime: %f, duedate: %f\n", task_queue[i].endTime, duedate);
        sumLateness += lateness[i];
        if(queue_task[i].endTime > max_endtime)
            max_endtime = queue_task[i].endTime;
    }

    // write lateness for plotting graph
    /* FILE *latenesslog;
    latenesslog = fopen("logs/lateness-log.txt", "w+");
    int count = 0;
    for(i = 0; i < number_of_tasks; i++){
        fprintf(latenesslog, "%f,", lateness[i]);
        count = count + 1;
        if(count == 6){
            count = 0;
            fprintf(latenesslog, "\n");
        }
    }
    fclose(latenesslog); */

    /* printf("------------------After simulation-------------------\n");
    printf("\t submit \t start_time \t runtime \t duedate \t end_time \t node\n");
    for(i = 0; i < 48; i++){
        double runtime = task_queue[i].endTime - task_queue[i].startTime;
        printf("\t %f \t %f \t %f \t %f \t %f \t %d\n", task_queue[i].submitTime, task_queue[i].startTime, runtime, list_duedate[i], task_queue[i].endTime, task_queue[i].node_allocation);
        // printf("\t %f \t \t %d \t \t %d \t \t %f \t %f\n", model_runtimes[i], model_cores[i], model_submit[i], task_queue[i].startTime, task_queue[i].endTime);
        // sleep(1);
    } */

    /* calculate AVG_bounded_slowdown */
    double AVGSlowdown = sumSlowdown / (number_of_tasks - NUM_INIT_TASKS);
    double AVGLateness = sumLateness / (number_of_tasks - NUM_INIT_TASKS);
    double Throughput = number_of_tasks * 3600 / max_endtime;

    /* write log file */
    // logfile = fopen("./logfile.out", 'w+');
    // fprintf(logfile, "-------------------------------\n");
    // fprintf(logfile, "AVGlateness = %f\n", AVGLateness);
    // fprintf(logfile, "Thoughput = %f\n", Throughput);
    // fprintf(logfile, "-------------------------------\n");
    // fclose(logfile);

    if(VERBOSE){
        XBT_INFO("Average bounded slowdown: %f", AVGSlowdown);
        XBT_INFO("Throughput: %f", Throughput);
        XBT_INFO("Average lateness: %f", AVGLateness);
        XBT_INFO("Simulation time %g", MSG_get_clock());
    }else if(!STATE){
        // printf("%f\n", AVGLateness);
        printf("%f\n", Throughput);
        // printf("%f\n", AVGSlowdown);
    }

    /* free memory */
    // free(slowdown);
    // free(lateness);
    // free(list_duedate);

    return res;
}

/* master function in the simgrid module */
int master(int argc, char *argv[]){

    int workers_count = 0;
    number_of_nodes = 0;
    msg_host_t *workers = NULL;
    msg_host_t task_manager = NULL;
    msg_task_t *todo = NULL;

    int i;
    int res = sscanf(argv[1], "%d", &workers_count);
    xbt_assert(res,"Invalid argument %s\n", argv[1]);
    // printf("\t [master] sscanf workers_count passed.\n");

    /* res = sscanf(argv[2], "%d", &number_of_nodes);
    xbt_assert(res, "Invalid argument %s\n", argv[2]);
    printf("\t [master] sscanf number_of_nodes passed.\n"); */

    /* call function: read model file */
    // printf("\t [master] call readModelFile()\n");
    readModelFile();

    orig_task_position = (int *) malloc((number_of_tasks - NUM_INIT_TASKS) * sizeof(int));

    int c = 0;
    int index = 0;
    for (i = NUM_INIT_TASKS; i < number_of_tasks; i++){    // number_of_tasks = 48
        index = c++;
        orig_task_position[index] = i;
    }

    /* call function: sort task queue */
    // printf("before sorting:\n");
    // printf("task \t submit \t cores \t mic \t runtime \t duedate\n");
    // for(i = 0; i < 48; i++){
    //     printf("%d \t %d \t %d \t %d \t %f \t %d\n", i , model_submit[i], model_cores[i], model_mic[i], model_runtimes[i], model_duedate[i]);
    // }

    sortTasksQueue(&model_runtimes[NUM_INIT_TASKS], &model_cores[NUM_INIT_TASKS], &model_submit[NUM_INIT_TASKS], &model_mic[NUM_INIT_TASKS], &model_duedate[NUM_INIT_TASKS], chosen_policy);
    
    // printf("after sorting:\n");
    // printf("\t submit \t cores \t mic \t runtime \t duedate\n");
    // for(i = 0; i < 48; i++){
    //     printf("%d \t %d \t %d \t %f \t %d\n", model_submit[i], model_cores[i], model_mic[i], model_runtimes[i], model_duedate[i]);
    // }

    p_master = MSG_process_self();
    {
        char sprintf_buffer[SPRINTF_BUFFER_SIZE];
        int node_number = 0;
        workers = xbt_new0(msg_host_t, workers_count);
        
        for(i = 0; i < workers_count; i++){
            node_number = (i + WORKERS_PER_NODE) / WORKERS_PER_NODE;
            sprintf(sprintf_buffer, "node-%d", node_number);
            workers[i] = MSG_get_host_by_name(sprintf_buffer);
            xbt_assert(workers[i] != NULL, "Unknown host %s. Stopping Now! ", sprintf_buffer);
        }
    }

    {
        task_manager = MSG_get_host_by_name("node-0");
        xbt_assert(task_manager != NULL, "Unknown host %s. Stopping Now! ", "node-0");
    }

    if(VERBOSE)
        XBT_INFO("Got %d workers and %d tasks to process", workers_count, number_of_tasks);

    /* Task creation */
    {
        // printf("[master] task_create\n");
        int j, k;
        todo = xbt_new0(msg_task_t, number_of_tasks);

        /* create a list of array to mark which core of node is busy */
        // printf("[master] busy_workers are creating...\n");
        for(i = 0; i < NUM_COMPUTE_NODES; i++)
            busy_workers[i] = (int *) calloc(workers_count, sizeof(int));

        queue_task = (struct task_t *) malloc(number_of_tasks * sizeof(struct task_t));
        busy_mic = (int *) calloc(NUM_COMPUTE_NODES, sizeof(int));

        /* variables for checking */
        bool is_resource_enough = false;
        int chosen_node = 0;

        /* array to count available cores on each node */
        int avalable_cores[NUM_COMPUTE_NODES];
        for(i = 0; i < NUM_COMPUTE_NODES; i++)
            avalable_cores[i] = 0;

        /* create a pointer linked to assignNode() function */
        int *assign_info;

        for(i = 0;  i < number_of_tasks; i++){
            // printf("[master] Job %d is being processed ...\n", i);
            // printf("\t  submit_time - %d | req_CPU - %d | req_MIC - %d \n", model_submit[i], model_cores[i], model_mic[i]);
            chosen_node = -1;
            do{
                while(MSG_get_clock() < model_submit[i]){ // this task has not arrived yet
                    MSG_process_sleep(model_submit[i] - MSG_get_clock());
                }

                for(j = 0; j < NUM_COMPUTE_NODES; j++){
                    // printf("\t Checking the availability on Node %d ...\n", j+1);
                    assign_info = assignNode(busy_workers[j], busy_mic, model_cores[i], model_mic[i], workers_count, j);
                    chosen_node = assign_info[0];
                    avalable_cores[j] = assign_info[1];
                    // printf("\t \t chosen_node = %d, available_cores on Node %d = %d...\n", chosen_node, j, avalable_cores[j]);
                    if (chosen_node != -1)
                        break;
                }
            
                is_resource_enough = (chosen_node != -1);
                
                if(!is_resource_enough){
                    if(VERBOSE){
                        XBT_INFO("Insuficient workers for Job %d need %d cpus, %d mics). Waiting.", i, model_cores[i], model_mic[i]);
                        for(j = 0; j < NUM_COMPUTE_NODES; j++)
                            XBT_INFO("Node %d: %d available CPU cores.", j, avalable_cores[j]);
                    }
                    MSG_process_suspend(p_master);
                }
            
            }while(!is_resource_enough);

            queue_task[i].numNodes = model_cores[i];
            queue_task[i].numMICs = model_mic[i];   // assign num of mics into numMICs
            queue_task[i].startTime = 0.0f;
            queue_task[i].endTime = 0.0f;
            queue_task[i].submitTime = model_submit[i];

            // assign duedate value into each job
            queue_task[i].duedate = model_duedate[i];
            queue_task[i].task_allocation = (int *) malloc(model_cores[i] * sizeof(int));
            queue_task[i].node_allocation = chosen_node;

            /* update busy_mic */
            if(model_mic[i] == 1){
                for(j = 0; j < NUM_COMPUTE_NODES; j++){
                    if(chosen_node == j)
                        busy_mic[j] = 1;
                }
            }

            /* update busy_workers */
            int count = 0;
            for (j = 0; j < workers_count; j++){
                for (k = 0; k < NUM_COMPUTE_NODES; k++){
                    if(chosen_node == k){
                        if(busy_workers[k][j] == 0){
                            queue_task[i].task_allocation[count] = j;
                            busy_workers[k][j] = 1;
                            count++;
                        }
                    }
                }
                
                if(count >= model_cores[i]){
                    break;
                }
            }

            msg_host_t self = MSG_host_self();
            double speed  = MSG_host_get_speed(self);
            double comp_size = model_runtimes[i] * speed;
            double comm_size = 1000.0f;
            char sprintf_buffer[64];

            if(i < NUM_INIT_TASKS){
                sprintf(sprintf_buffer, "Job_%d", i);
            }else{
                sprintf(sprintf_buffer, "Job_%d", orig_task_position[i - NUM_INIT_TASKS]);
            }

            /* Task create */
            todo[i] = MSG_task_create(sprintf_buffer, comp_size, comm_size, &queue_task[i]);

            if(VERBOSE)
                XBT_INFO("Dispatching %s [r=%.1f, c=%d, s=%d, m=%d] to node %d", todo[i]->name, model_runtimes[i], model_cores[i], model_submit[i], model_mic[i], chosen_node);
            
            /* Task send */
            MSG_task_send(todo[i], MSG_host_get_name(workers[i]));

            if(VERBOSE)
                XBT_INFO("Sent");

            if(i == NUM_INIT_TASKS - 1){
                t0 = MSG_get_clock();

                if(STATE){
                    float * elapsed_times = (float *) calloc(workers_count, sizeof(float));
                    for(j = 0; j <= i; j++){
                        if(queue_task[j].endTime == 0.0f){
                            float task_elapsed_time = MSG_get_clock() - queue_task[j].startTime;
                            if(j == i)
                                task_elapsed_time = 0.01f;

                            for(k = 0; k < model_cores[j]; k++){
                                elapsed_times[(queue_task[j].task_allocation[k])] = task_elapsed_time;
                            }
                        }
                    }

                    for(j = 0; j < workers_count; j++){
                        if(j < workers_count - 1)
                            printf("%f, ", elapsed_times[j]);
                        else
                            printf("%f\n", elapsed_times[j]);
                    }
                    break;
                }
            }
        }

        if(STATE){
            if(VERBOSE)
                XBT_INFO("All tasks have been dispatched. Let's tell everybody the computation is over.");
            for(i = NUM_INIT_TASKS; i < num_managers; i++){
                msg_task_t finalize = MSG_task_create("finalize", 0, 0, FINALIZE);
                MSG_task_send(finalize, MSG_host_get_name(workers[i]));
            }
        }

        if(VERBOSE){
            XBT_INFO("Goodbye!");
            free(workers);
            free(todo);
            return 0;
        }
    }

    /* free memory */
    for(i = 0; i < NUM_COMPUTE_NODES; i++)
        free(busy_workers[i]);
    free(queue_task);
    free(busy_mic);
    
    return 0;
}

/* task manager function in the simgrid module */
int taskManager(int argc, char *argv[]){

    msg_task_t task = NULL;
    struct task_t *_task = NULL;
    int i, j;
    int res;
    res = MSG_task_receive(&task, MSG_host_get_name(MSG_host_self()));
    xbt_assert(res == MSG_OK, "MSG_task_receive failed");
    _task = (struct task_t*) MSG_task_get_data(task);

    if(VERBOSE)
        XBT_INFO("Received \"%s\"", MSG_task_get_name(task));

    if(!strcmp(MSG_task_get_name(task), "finalize")){
        MSG_task_destroy(task);
        return 0;
    }

    if(VERBOSE)
        XBT_INFO("Processing \"%s\"", MSG_task_get_name(task));

    _task->startTime = MSG_get_clock();
    MSG_task_execute(task);
    _task->endTime = MSG_get_clock();

    if(VERBOSE)
        XBT_INFO("\"%s\" done ", MSG_task_get_name(task));

    int * allocation = _task->task_allocation;
    int num_cores = _task->numNodes;
    int mics = _task->numMICs;

    /* free busy_workers for each node after job finished */
    for(i = 0; i < num_cores; i++){
        for(j = 0; j < NUM_COMPUTE_NODES; j++){
            if(_task->node_allocation == j)
                busy_workers[j][allocation[i]] = 0;
        }
    }

    /* free busy_mic for each node */
    if(mics == 1){
        for(j = 0; j < NUM_COMPUTE_NODES; j++){
            if(_task->node_allocation == j)
                busy_mic[j] = 0;
        }
    }

    MSG_task_destroy(task);
    task = NULL;
    MSG_process_resume(p_master);

    if(VERBOSE)
        XBT_INFO("I am done, see you!");

    return 0;
}

/* assignNode function */
int * assignNode(int *workers_status, int *mic_status, int cores, int mic, int num_workers, int node){

    static int result[2];
    int available_cores = 0;
    int chosen_node = -1;
    int j;
    if(mic == 1){
        if(mic_status[node] == 0){
            available_cores = 0;
            for(j = 0; j < num_workers; j++){
                if(workers_status[j] == 0){
                    available_cores++;
                    if(available_cores == cores){
                        chosen_node = node;    // this node is chosen
                    }
                }
            }
        }else{
            result[0] = chosen_node;
            result[1] = available_cores;
            return result;
        }
    }else{
        available_cores = 0;
        for(j = 0; j < num_workers; j++){
            if(workers_status[j] == 0){
                available_cores++;
                if(available_cores == cores){
                    chosen_node = node;    // this node is chosen
                }
            }
        }
    }
    result[0] = chosen_node;
    result[1] = available_cores;

    return result;
}

/* Read Model file function */
void readModelFile(void){
    model_runtimes = (double *) malloc((number_of_tasks) * sizeof(double));
    model_submit = (int *) malloc((number_of_tasks) * sizeof(int));
    model_cores = (int *) malloc((number_of_tasks) * sizeof(int));
    model_mic = (int *) malloc((number_of_tasks) * sizeof(int));
    model_duedate = (int *)malloc((number_of_tasks) * sizeof(double));
    int task_count = 0;

    FILE* stream = fopen("initial-simulation-submit.csv", "r");
    char line[1024];
    while(fgets(line, 1024, stream)){
        char *tmp = strdup(line);
        model_runtimes[task_count] = atof(getfield(tmp, 1));
        free(tmp);
        tmp = strdup(line);
        model_cores[task_count] = atoi(getfield(tmp, 2));
        free(tmp);
        tmp = strdup(line);
        model_submit[task_count] = atoi(getfield(tmp, 3));
        free(tmp);
        tmp = strdup(line);
        model_mic[task_count] = atoi(getfield(tmp, 4));
        free(tmp);
        tmp = strdup(line);
        model_duedate[task_count] = atoi(getfield(tmp, 5));
        // NOTE strtok clobbers tmp
        task_count++;
        free(tmp);
    }

    /* int i;
    printf("\t submit  cpus \t runtime \t mic \t |duedate| \n");
    for(i = 0; i < 50; i++){
        printf("\t %d \t %d \t %.5f \t %d \t %d\n", model_submit[i], model_cores[i], model_runtimes[i], model_mic[i], model_duedate[i]);
        // sleep(1);
    } */
}

/* Get field function */
const char *getfield(char *line, int num){
    const char *tok;
    for(tok = strtok(line, ","); tok && *tok; tok = strtok(NULL, ",\n")){
        if(!--num)
            return tok;
    }
    return NULL;
}

/* Define inv function */
double inv(int x){
    double result = 1.0 / (x + 1e-10);
    return result;
}

/* Sort Task Queue */
void sortTasksQueue(double *runtimes, int *cores, int *submit, int *mic, int *duedate, int policy){

    int i, j;
    int N = number_of_tasks - NUM_INIT_TASKS;
    /* FCFS policy */
    if(policy == FCFS)
        return;

    /* LPT policy */
    if(policy == LPT){
        double r_buffer;    // runtimes
        int c_buffer;   // cores
        int s_buffer;   // submit time
        int m_buffer;   // mic
        int d_buffer;   // duedate
        int p_buffer;   // position
        for(i = 0; i < N; i++){
            for(j = 0; j < N; j++){
                if (runtimes[i] > runtimes[j]){
                    r_buffer = runtimes[i];
                    c_buffer = cores[i];
                    s_buffer = submit[i];
                    m_buffer = mic[i];
                    d_buffer = duedate[i];
                    p_buffer = orig_task_position[i];

                    runtimes[i] = runtimes[j];
                    cores[i] = cores[j];
                    submit[i] = submit[j];
                    mic[i] = mic[j];
                    duedate[i] = duedate[j];
                    orig_task_position[i] = orig_task_position[j];

                    runtimes[j] = r_buffer;
                    cores[j] = c_buffer;
                    submit[j] = s_buffer;
                    mic[j] = m_buffer;
                    duedate[j] = d_buffer;
                    orig_task_position[j] = p_buffer;
                }
            }
        }
        return;
    }

    /* SPT policy */
    if(policy == SPT){
        double r_buffer;    // runtimes
        int c_buffer;   // core
        int s_buffer;   // submit
        int m_buffer;   // mic
        int d_buffer;   // duedate
        int p_buffer;   // position
        for(i = 0; i < N; i++){
            for(j = 0; j < N; j++){
                if (runtimes[i] < runtimes[j]){
                    r_buffer = runtimes[i];
                    c_buffer = cores[i];
                    s_buffer = submit[i];
                    m_buffer = mic[i];
                    d_buffer = duedate[i];
                    p_buffer = orig_task_position[i];

                    runtimes[i] = runtimes[j];
                    cores[i] = cores[j];
                    submit[i] = submit[j];
                    mic[i] = mic[j];
                    duedate[i] = duedate[j];
                    orig_task_position[i] = orig_task_position[j];

                    runtimes[j] = r_buffer;
                    cores[j] = c_buffer;
                    submit[j] = s_buffer;
                    mic[j] = m_buffer;
                    duedate[j] = d_buffer;
                    orig_task_position[j] = p_buffer;
                }
            }
        }
        return;
    }

    /* EDD policy */
    if(policy == EDD){
        double r_buffer;    // runtimes
        int c_buffer;   // core
        int s_buffer;   // submit
        int m_buffer;   // mic
        int d_buffer;   // duedate
        int p_buffer;   // position
        for(i = 0; i < N; i++){
            for(j = 0; j < N; j++){
                if (duedate[i] < duedate[j]){
                    r_buffer = runtimes[i];
                    c_buffer = cores[i];
                    s_buffer = submit[i];
                    m_buffer = mic[i];
                    d_buffer = duedate[i];
                    p_buffer = orig_task_position[i];

                    runtimes[i] = runtimes[j];
                    cores[i] = cores[j];
                    submit[i] = submit[j];
                    mic[i] = mic[j];
                    duedate[i] = duedate[j];
                    orig_task_position[i] = orig_task_position[j];

                    runtimes[j] = r_buffer;
                    cores[j] = c_buffer;
                    submit[j] = s_buffer;
                    mic[j] = m_buffer;
                    duedate[j] = d_buffer;
                    orig_task_position[j] = p_buffer;
                }
            }
        }
        return;
    }

    /* use for other algorithms to calculate the priority for each job */
    double* h_values = (double*) calloc(N, sizeof(double));
    double* r_temp = (double*) calloc(N, sizeof(double));
    int* c_temp = (int*) calloc(N, sizeof(int));
    int* s_temp = (int*) calloc(N, sizeof(int));
    int* m_temp = (int*) calloc(N, sizeof(int));
    int* d_temp = (int*) calloc(N, sizeof(int));
    int* p_temp = (int*) calloc(N, sizeof(int));
    int max_arrive = 0;

    /* find a job with the latest submit time */
    for(i = 0; i < N; i++){
        if(submit[i] > max_arrive)
            max_arrive = submit[i];
    }

    /* calculate the score for each job */
    int queue_score = 0;
    for(i = 0; i < N;i++){
        queue_score = max_arrive - submit[i]; //priority score for the arrival time (bigger = came first)
        switch(policy){
            case WFP3:
                h_values[i] = pow(queue_score/runtimes[i], 3) * cores[i];
                break;
            case UNICEF:
	            h_values[i] = queue_score/(log2((double)cores[i]) * runtimes[i]);
                // printf("task %d: h_values = %f\n", (i + NUM_TASKS_STATE), h_values[i]);
	            break;
            case CANDIDATE1:
                // h_values[i] = log10(runtimes[i]) * sqrt(cores[i]); //candidate 1 (increasing order)
                // h_values[i] = (0.0075611 * log10(runtimes[i])) + (0.0113013 * log10(cores[i])); //candidate 1 (increasing order)
                h_values[i] = duedate[i] * (-0.0000000314 * runtimes[i] * (-0.0036668288 * cores[i]) + mic[i]) + (0.0103053379 * log10(submit[i]));
                break;
            case CANDIDATE2:
                // h_values[i] = log10(runtimes[i]) * cores[i]; //candidate 2 
                h_values[i] = (0.0066197 * log10(runtimes[i])) + (0.0039650 * sqrt(cores[i])); //candidate 2
                // h_values[i] = (0.0081926 * log10(runtimes[i])) + (0.0173701* (1.0 / cores[i])); //candidate 2
                break;
            case CANDIDATE3:
                h_values[i] = 49.5155372 * sqrt(runtimes[i]) / (129.3129007 * (inv(cores[i]))) + 219.4333513 * log10(submit[i]) / (7004.1400155 * log10(duedate[i]) + mic[i]);
                // printf("job %d: h_values = %.5f\n", i, h_values[i]);
                break;
            case CANDIDATE4:
                h_values[i] = 6.9867922 * runtimes[i] / (375.3895129 * (inv(cores[i]))) + 429.4184138 * log10(submit[i]) / (13121.1874123 * log10(duedate[i]) + mic[i]);
                break;
        }
    }
    
    if(policy == WFP3 || policy == UNICEF){
        double max_val = 0.0;
        int max_index = 0;
        for(i = 0; i < N;i++){
            max_val = 0.0;  
            for(j = 0; j < N; j++){
                if(h_values[j] >= max_val){
                    max_val = h_values[j];
                    max_index = j;
                }
            }
            // printf("task %d is moved to position = %d\n", (i+NUM_TASKS_STATE), (NUM_TASKS_STATE + max_index));
            r_temp[i] = runtimes[max_index];
            c_temp[i] = cores[max_index];
            s_temp[i] = submit[max_index];
            m_temp[i] = mic[max_index];
            d_temp[i] = duedate[max_index];
            p_temp[i] = NUM_INIT_TASKS + max_index;
            h_values[max_index] = -1.0;
        }
    }else if(policy == CANDIDATE1 || policy == CANDIDATE2 || policy == CANDIDATE3 || policy == CANDIDATE4){
        double min_val = 1e20;
        int min_index = 0;
        for(i = 0; i < N;i++){
            min_val = 1e20;  
            for(j = 0; j < N; j++){		
                if(h_values[j] <= min_val){
                    min_val = h_values[j];
                    min_index = j;	
                }
            }
            r_temp[i] = runtimes[min_index];
            c_temp[i] = cores[min_index];
            s_temp[i] = submit[min_index];
            m_temp[i] = mic[min_index];
            d_temp[i] = duedate[min_index];
            p_temp[i] = NUM_INIT_TASKS + min_index;
            h_values[min_index] = 1e20;	
        }
    }
    
    for(i = 0; i < N;i++){
        runtimes[i] = r_temp[i];
        cores[i] = c_temp[i];
        submit[i] = s_temp[i];
        mic[i] = m_temp[i];
        duedate[i] = d_temp[i];
        orig_task_position[i] = p_temp[i];
    }
    
    free(r_temp);
    free(c_temp);
    free(s_temp);
    free(m_temp);
    free(d_temp);
    free(p_temp);
    free(h_values);
}