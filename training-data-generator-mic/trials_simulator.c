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

XBT_LOG_NEW_DEFAULT_CATEGORY(msg_test,
                             "Messages specific for this msg example");

void sortTasksQueue(double *runtimes, int *cores, int *submit, int *mic, int *duedate, int policy);
const char *getfield(char *line, int num);
void readModelFile(void);
int master(int argc, char *argv[]);
int taskManager(int argc, char *argv[]);
int * assignNode(int *workers_status, int *mic_status, int cores, int mic, int num_workers, int node);
msg_error_t test_all(const char *platform_file, const char *application_file);

#define FINALIZE ((void*)221297)        /* a magic number to tell people to stop working */

#define MAX_TASKS 1024
#define WORKERS_PER_NODE 1
#define MAX_TASK_TYPES 5 
#define TERA 1e12
#define MEGA 1e6
#define TAO 10
#define MODEL_NUM_TASKS 32
#define NUM_TASKS_STATE 16
#define NUM_COMPUTE_NODES 3

#define FCFS 0
#define LPT 1
#define WFP3 2
#define UNICEF 3
#define CANDIDATE1 4
#define CANDIDATE2 5
#define SPT 6
#define EDD 7
#define DCRUC 8

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

int VERBOSE = 0;    /* for debug */
int STATE = 0;

double *model_runtimes; /* array to store a list jobs in lublib model */
int *model_submit;
int *model_cores;
int *model_mic;
int *model_duedate;

int *orig_task_position;    /* to store the positions of job */

double *slowdown;   /* array to store slowdown value of each job */
double *list_duedate; /* array to store duedate values */
double *lateness;   /* array to store lateness values */

struct task_t *task_queue = NULL;

msg_process_t p_master;

int chosen_policy = FCFS;   /* default policy fo scheduler */
/* to mark busy cores when running jobs */
/* each busy worker represent a compute node */
int *busy_workers1; /* node1 */
int *busy_workers2; /* node2 */
int *busy_workers3; /* node3 */
int *busy_mic; /* mic on nodes */

int num_managers = MODEL_NUM_TASKS + NUM_TASKS_STATE;

double *sched_task_placement;
int number_of_tasks = MODEL_NUM_TASKS + NUM_TASKS_STATE;
double t0 = 0.0f;

FILE * logfile;

/* Main function */
int main(int argc, char *argv[]){
    msg_error_t res = MSG_OK;
    int i;

    // printf("[Main function] MSG_init\n");
    MSG_init(&argc, argv);
    if (argc < 3) {
        printf("Usage: %s platform_file deployment_file [-verbose]\n", argv[0]);
        printf("example: %s msg_platform.xml msg_deployment.xml -verbose\n", argv[0]);
        exit(1);
    }

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
            }
            if (strcmp(argv[i], "-spt") == 0){
                chosen_policy = SPT;
            }
	        if (strcmp(argv[i], "-wfp3") == 0){
                chosen_policy = WFP3;
            }
	        if (strcmp(argv[i], "-unicef") == 0){
                chosen_policy = UNICEF;
            }
            if (strcmp(argv[i], "-edd") == 0){
                chosen_policy = EDD;
            }
            if (strcmp(argv[i], "-c1") == 0){
                chosen_policy = CANDIDATE1;
            }
            if (strcmp(argv[i], "-c2") == 0){
                chosen_policy = CANDIDATE2;
            }
        }
    }
    
    // printf("[Main function] call test_all()\n");
    res = test_all(argv[1], argv[2]);

    if (res == MSG_OK){
        // printf("[Main function] return 0\n");
        return 0;
    }else{
        // printf("[Main function] return 1\n");
        return 1;
    }
}

/* assignNode function */
int * assignNode(int *workers_status, int *mic_status, int cores, int mic, int num_workers, int node){
    // printf("[Debug] assignNode: cores = %d, mic = %d, node = %d, busy_mic[n1,n2,n3] = [%d,%d,%d]\n", model_cores, model_mic, node, busy_mic[0], busy_mic[1], busy_mic[2]);
    static int result[2];
    int available_node = 0;
    int chosen_node = 0;
    int j;
    if(mic == 1){
        if(mic_status[node-1] == 0){
            available_node = 0;
            for(j = 0; j < num_workers; j++){
                if(workers_status[j] == 0){
                    available_node++;
                    if(available_node == cores){
                        chosen_node = node;    // this node is chosen
                    }
                }
            }
        }else{
            result[0] = chosen_node;
            result[1] = available_node;
            return result;
        }
    }else{
        available_node = 0;
        for(j = 0; j < num_workers; j++){
            if(workers_status[j] == 0){
                available_node++;
                if(available_node == cores){
                    chosen_node = node;    // this node is chosen
                }
            }
        }
    }
    result[0] = chosen_node;
    result[1] = available_node;
    return result;
}

/* Test function */
msg_error_t test_all(const char *platform_file, const char *application_file){

    msg_error_t res = MSG_OK;
    int i;

    /* Simulation setting */
    {
        // printf("[test_all] simulation setting\n");
        MSG_config("host/model", "default");
        MSG_create_environment(platform_file);
    }

    /* Application deployment */
    {
        // printf("[test_all] register master\n");
        MSG_function_register("master", master);

        // printf("[test_all] register taskManager\n");
        MSG_function_register("taskManager", taskManager);

        // printf("[test_all] launch application\n");
        MSG_launch_application(application_file);
        
        // printf("[test_all] create process taskManager with num_managers = %d\n", num_managers);
        char sprintf_buffer[64];
        for(i = 0; i < num_managers; i++){
            sprintf(sprintf_buffer, "node-%d", i + 1);
            MSG_process_create("taskManager", taskManager, NULL, MSG_get_host_by_name(sprintf_buffer));
        }
    }

    // printf("[test_all] call MSG_main()\n");
    res = MSG_main();

    // printf("[Application deployment-passed]\n");

    double sumSlowdown = 0.0f;
    slowdown = (double *) calloc(MODEL_NUM_TASKS, sizeof(double));
    int _count = 0;
    for(i = NUM_TASKS_STATE; i < number_of_tasks; i++){
        double waitTime = task_queue[i].startTime - task_queue[i].submitTime;
        double runTime = task_queue[i].endTime - task_queue[i].startTime;
        double quocient = runTime >= TAO ? runTime : TAO;
        double slow = (waitTime + runTime) / quocient;
        slowdown[_count] = slow >= 1.0f ? slow : 1.0f;
        sumSlowdown += slowdown[_count];
        _count++;
    }

    // calculate lateness
    double sumLateness = 0.0f;
    double max_endtime = 0.0f;
    lateness = (double *) calloc(MODEL_NUM_TASKS + NUM_TASKS_STATE, sizeof(double));
    list_duedate = (double *) calloc(MODEL_NUM_TASKS + NUM_TASKS_STATE, sizeof(double));
    for(i = NUM_TASKS_STATE; i < number_of_tasks; i++){
        // double runTime = task_queue[i].endTime - task_queue[i].startTime;
        double duedate = task_queue[i].duedate;
        list_duedate[i] = duedate;
        lateness[i] = task_queue[i].endTime -  duedate;
        // printf("endtime: %f, duedate: %f\n", task_queue[i].endTime, duedate);
        sumLateness += lateness[i];
        if(task_queue[i].endTime > max_endtime)
            max_endtime = task_queue[i].endTime;
    }

    // write lateness for plotting graph
    FILE *latenesslog;
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
    fclose(latenesslog);

    /* printf("------------------After simulation-------------------\n");
    printf("\t submit \t start_time \t runtime \t duedate \t end_time \t node\n");
    for(i = 0; i < 48; i++){
        double runtime = task_queue[i].endTime - task_queue[i].startTime;
        printf("\t %f \t %f \t %f \t %f \t %f \t %d\n", task_queue[i].submitTime, task_queue[i].startTime, runtime, list_duedate[i], task_queue[i].endTime, task_queue[i].node_allocation);
        // printf("\t %f \t \t %d \t \t %d \t \t %f \t %f\n", model_runtimes[i], model_cores[i], model_submit[i], task_queue[i].startTime, task_queue[i].endTime);
        // sleep(1);
    } */

    /* calculate AVG_bounded_slowdown */
    double AVGSlowdown = sumSlowdown / MODEL_NUM_TASKS;
    double AVGLateness = sumLateness / MODEL_NUM_TASKS;
    double Throughput = number_of_tasks * 3600 / max_endtime;

    /* write log file */
    // logfile = fopen("./logfile.out", 'w+');
    // fprintf(logfile, "-------------------------------\n");
    // fprintf(logfile, "AVGlateness = %f\n", AVGLateness);
    // fprintf(logfile, "Thoughput = %f\n", Throughput);
    // fprintf(logfile, "-------------------------------\n");
    // fclose(logfile);

    if(VERBOSE){
        // XBT_INFO("Average bounded slowdown: %f", AVGSlowdown);
        // XBT_INFO("Throughput: %f", Throughput);
        XBT_INFO("Average lateness: %f", AVGLateness);
        XBT_INFO("Simulation time %g", MSG_get_clock());
    }else if(!STATE){
        // printf("%f\n", AVGSlowdown);
        printf("AVGLateness = %f\n", AVGLateness);
        printf("Throughput = %f\n", Throughput);
        printf("AVGSlowdown = %f\n", AVGSlowdown);
        // printf("%f\n", Throughput);
    }

    return res;
}

/* Emitter function */
int master(int argc, char *argv[]){

    // printf("----------------start-master----------------\n");

    int workers_count = 0;
    msg_host_t *workers = NULL;
    msg_host_t task_manager = NULL;
    msg_task_t *todo = NULL;

    int i;
    int res = sscanf(argv[1], "%d", &workers_count);
    xbt_assert(res,"Invalid argument %s\n", argv[1]);
    // printf("[master] sscanf argv[1]=%s, res = %d\n", argv[1], res);
    // sleep(3);

    /* call function: read model file */
    // printf("[master] call readModelFile()\n");
    readModelFile();
    // sleep(3);

    // printf("[master] assign orig_task_position\n");
    orig_task_position = (int *) malloc(MODEL_NUM_TASKS * sizeof(int));
    int c = 0;
    int index = 0;
    for (i = NUM_TASKS_STATE; i < number_of_tasks; i++){    // number_of_tasks = 48
        index = c++;
        // printf("\t orig_task_position[%d] = %d\n", index, i);
        orig_task_position[index] = i;
        // sleep(1);
    }

    /* call function: sort task queue */
    // printf("before sorting:\n");
    // printf("task \t submit \t cores \t mic \t runtime \t duedate\n");
    // for(i = 0; i < 48; i++){
    //     printf("%d \t %d \t %d \t %d \t %f \t %d\n", i , model_submit[i], model_cores[i], model_mic[i], model_runtimes[i], model_duedate[i]);
    // }

    sortTasksQueue(&model_runtimes[NUM_TASKS_STATE], &model_cores[NUM_TASKS_STATE], &model_submit[NUM_TASKS_STATE], &model_mic[NUM_TASKS_STATE], &model_duedate[NUM_TASKS_STATE], chosen_policy);
    
    // printf("after sorting:\n");
    // printf("\t submit \t cores \t mic \t runtime \t duedate\n");
    // for(i = 0; i < 48; i++){
    //     printf("%d \t %d \t %d \t %f \t %d\n", model_submit[i], model_cores[i], model_mic[i], model_runtimes[i], model_duedate[i]);
    // }

    /* process organization */
    // printf("[master] process organization\n");
    p_master = MSG_process_self();
    {
        char sprintf_buffer[64];
        int node_number = 0;
        workers = xbt_new0(msg_host_t, workers_count);
        // printf("\t workers_count = %d\n", workers_count);
        for(i = 0; i < workers_count; i++){
            node_number = (i + WORKERS_PER_NODE) / WORKERS_PER_NODE;
            sprintf(sprintf_buffer, "node-%d", node_number);
            workers[i] = MSG_get_host_by_name(sprintf_buffer);
            // printf("\t sprintf_buffer - %d\n", node_number);
            xbt_assert(workers[i] != NULL, "Unknown host %s. Stopping Now! ", sprintf_buffer);
        }
    }
    // sleep(2);

    // printf("[master] task_manager get_host_by_name\n");
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

        busy_workers1 = (int *) calloc(workers_count, sizeof(int));
        busy_workers2 = (int *) calloc(workers_count, sizeof(int));
        busy_workers3 = (int *) calloc(workers_count, sizeof(int));
        task_queue = (struct task_t *) malloc(number_of_tasks * sizeof(struct task_t));
        busy_mic = (int *) calloc(NUM_COMPUTE_NODES, sizeof(int));

        /* variables for checking */
        bool is_resource_enough = false;
        int chosen_node = 0;
        int available_node1 = 0;
        int available_node2 = 0;
        int available_node3 = 0;
        /* create a pointer linked to assignNode() function */
        int *assign_info;

        for(i = 0;  i < number_of_tasks; i++){
            chosen_node = 0;
            // int available_node1, available_node2, available_node3;
            do{
                while(MSG_get_clock() < model_submit[i]){ // this task has not arrived yet
                    MSG_process_sleep(model_submit[i] - MSG_get_clock());
                }
                /* busy_mic = [0, 0, 0], 0 is free, 1 is occupied */

                // check node 1
                assign_info = assignNode(busy_workers1, busy_mic, model_cores[i], model_mic[i], workers_count, 1);
                chosen_node = assign_info[0];
                available_node1 = assign_info[1];
                // printf("[Debug] chosen_node = %d, available_node1 = %d\n", chosen_node, available_node1);

                // check node 2
                if(chosen_node == 0){
                    assign_info = assignNode(busy_workers2, busy_mic, model_cores[i], model_mic[i], workers_count, 2);
                    chosen_node = assign_info[0];
                    available_node2 = assign_info[1];
                    // printf("[Debug] chosen_node = %d, available_node2 = %d\n", chosen_node, available_node1);
                }

                // check node 3
                if(chosen_node == 0){
                    assign_info = assignNode(busy_workers3, busy_mic, model_cores[i], model_mic[i], workers_count, 3);
                    chosen_node = assign_info[0];
                    available_node3 = assign_info[1];
                    // printf("[Debug] chosen_node = %d, available_node3 = %d\n", chosen_node, available_node1);
                }     
            
                is_resource_enough = (chosen_node != 0);
                // if(available_node < model_cores[i]){
                if(!is_resource_enough){
                    if(VERBOSE)
                        XBT_INFO("Insuficient workers for task \"%d\" (node 1 %d, node2 %d, node3 %d available workers. need %d cpus, %d mics). Waiting.", i, 
                            available_node1, available_node2, available_node3, model_cores[i], model_mic[i]);
                    MSG_process_suspend(p_master);
                }
            // }while(available_nodes < model_cores[i]);
            }while(!is_resource_enough);

            task_queue[i].numNodes = model_cores[i];
            task_queue[i].numMICs = model_mic[i];   // assign num of mics into numMICs
            task_queue[i].startTime = 0.0f;
            task_queue[i].endTime = 0.0f;
            task_queue[i].submitTime = model_submit[i];
            // assign duedate value into each job
            task_queue[i].duedate = model_duedate[i];
            task_queue[i].task_allocation = (int *) malloc(model_cores[i] * sizeof(int));
            task_queue[i].node_allocation = chosen_node;

            /* update busy_mic */
            if(model_mic[i] == 1){
                if(chosen_node == 1)
                    busy_mic[0] = 1;
                else if(chosen_node == 2)
                    busy_mic[1] = 1;
                else if(chosen_node == 3)
                    busy_mic[2] = 1;
            }

            /* update busy_workers */
            int count = 0;
            for (j = 0; j < workers_count; j++){
                if(chosen_node == 1){
                    if(busy_workers1[j] == 0){
                        task_queue[i].task_allocation[count] = j;
                        busy_workers1[j] = 1;
                        count++;
                    }
                }else if(chosen_node == 2){
                    if(busy_workers2[j] == 0){
                        task_queue[i].task_allocation[count] = j;
                        busy_workers2[j] = 1;
                        count++;
                    }
                }else{
                    if(busy_workers3[j] == 0){
                        task_queue[i].task_allocation[count] = j;
                        busy_workers3[j] = 1;
                        count++;
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
            if(i < NUM_TASKS_STATE){
                sprintf(sprintf_buffer, "Task_%d", i);
            }else{
                sprintf(sprintf_buffer, "Task_%d", orig_task_position[i - NUM_TASKS_STATE]);
            }

            /* Task create */
            todo[i] = MSG_task_create(sprintf_buffer, comp_size, comm_size, &task_queue[i]);

            if(VERBOSE)
                XBT_INFO("Dispatching \"%s\" [r=%.1f, c=%d, s=%d, m=%d] to node %d", todo[i]->name, model_runtimes[i], model_cores[i], model_submit[i], model_mic[i], chosen_node);
            
            /* Task send */
            MSG_task_send(todo[i], MSG_host_get_name(workers[i]));

            if(VERBOSE)
                XBT_INFO("Sent");

            if(i == NUM_TASKS_STATE - 1){
                t0 = MSG_get_clock();

                if(STATE){
                    float * elapsed_times = (float *) calloc(workers_count, sizeof(float));
                    for(j = 0; j <= i; j++){
                        if(task_queue[j].endTime == 0.0f){
                            float task_elapsed_time = MSG_get_clock() - task_queue[j].startTime;
                            if(j == i)
                                task_elapsed_time = 0.01f;

                            for(k = 0; k < model_cores[j]; k++){
                                elapsed_times[(task_queue[j].task_allocation[k])] = task_elapsed_time;
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
            for(i = NUM_TASKS_STATE; i < num_managers; i++){
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

    // printf("---------------end-master-----------------\n");
    
    return 0;
}

/* Receiver function */
int taskManager(int argc, char *argv[]){

    // printf("----------------start-taskManager----------------\n");

    msg_task_t task = NULL;
    struct task_t *_task = NULL;
    int i;
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
    int n = _task->numNodes;
    int mics = _task->numMICs;

    /* free busy_workers for each node after job finished */
    for(i = 0; i < n; i++){
        if(_task->node_allocation == 1)
            busy_workers1[allocation[i]] = 0;
        else if(_task->node_allocation == 2)
            busy_workers2[allocation[i]] = 0;
        else{
            busy_workers3[allocation[i]] = 0;
        }
    }

    /* free busy_mic for each node */
    if(mics == 1){
        if(_task->node_allocation == 1){
            busy_mic[0] = 0;
        }
        else if(_task->node_allocation == 2){
            busy_mic[1] = 0;
        }
        else{
            busy_mic[2] = 0;
        }
    }


    MSG_task_destroy(task);
    task = NULL;
    MSG_process_resume(p_master);

    if(VERBOSE)
        XBT_INFO("I am done, see you!");

    // printf("---------------end-taskManager-----------------\n");

    return 0;
}

/* Read Model file function */
void readModelFile(void){
    model_runtimes = (double *) malloc((MODEL_NUM_TASKS + NUM_TASKS_STATE) * sizeof(double));
    model_submit = (int *) malloc((MODEL_NUM_TASKS + NUM_TASKS_STATE) * sizeof(int));
    model_cores = (int *) malloc((MODEL_NUM_TASKS + NUM_TASKS_STATE) * sizeof(int));
    model_mic = (int *) malloc((MODEL_NUM_TASKS + NUM_TASKS_STATE) * sizeof(int));
    model_duedate = (int *)malloc((MODEL_NUM_TASKS + NUM_TASKS_STATE) * sizeof(double));
    int task_count = 0;

    FILE* stream = fopen("current-simulation.csv", "r");
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
    }

    /*
    int i;
    printf("\t submit  cpus \t runtime \t mic \t |duedate| \n");
    for(i = 0; i < 48; i++){
        printf("\t %d \t %d \t %.5f \t %d \t %d\n", model_submit[i], model_cores[i], model_runtimes[i], model_mic[i], model_duedate[i]);
        // sleep(1);
    }*/

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

/* Sort Task Queue */
void sortTasksQueue(double *runtimes, int *cores, int *submit, int *mic, int *duedate, int policy){
    int i, j;
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
        for(i = 0; i < MODEL_NUM_TASKS; i++){
            for(j = 0; j < MODEL_NUM_TASKS; j++){
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
        for(i = 0; i < MODEL_NUM_TASKS; i++){
            for(j = 0; j < MODEL_NUM_TASKS; j++){
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
        for(i = 0; i < MODEL_NUM_TASKS; i++){
            for(j = 0; j < MODEL_NUM_TASKS; j++){
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
    double* h_values = (double*) calloc(MODEL_NUM_TASKS, sizeof(double));
    double* r_temp = (double*) calloc(MODEL_NUM_TASKS, sizeof(double));
    int* c_temp = (int*) calloc(MODEL_NUM_TASKS, sizeof(int));
    int* s_temp = (int*) calloc(MODEL_NUM_TASKS, sizeof(int));
    int* m_temp = (int*) calloc(MODEL_NUM_TASKS, sizeof(int));
    int* d_temp = (int*) calloc(MODEL_NUM_TASKS, sizeof(int));
    int* p_temp = (int*) calloc(MODEL_NUM_TASKS, sizeof(int));
    int max_arrive = 0;
    /* find a job with the latest submit time */
    for(i = 0; i < MODEL_NUM_TASKS; i++){
        if(submit[i] > max_arrive)
            max_arrive = submit[i];
    }
    /* calculate the score for each job */
    int queue_score = 0;
    for(i = 0; i < MODEL_NUM_TASKS;i++){
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
                //h_values[i] = log10(runtimes[i]) * sqrt(cores[i]); //candidate 1 (increasing order)
                // h_values[i] = (0.0075611 * log10(runtimes[i])) + (0.0113013 * log10(cores[i])); //candidate 1 (increasing order)
                h_values[i] = duedate[i] * (-0.0000000314 * runtimes[i] * (-0.0036668288 * cores[i])) + (0.0103053379 * log10(submit[i]));
                break;
            case CANDIDATE2:
                //h_values[i] = log10(runtimes[i]) * cores[i]; //candidate 2 
                h_values[i] = (0.0066197 * log10(runtimes[i])) + (0.0039650 * sqrt(cores[i])); //candidate 2 
                //h_values[i] = (0.0081926 * log10(runtimes[i])) + (0.0173701* (1.0 / cores[i])); //candidate 2 
                break;
        }
    }
    
    if(policy == WFP3 || policy == UNICEF){
        double max_val = 0.0;
        int max_index = 0;
        for(i = 0; i < MODEL_NUM_TASKS;i++){
            max_val = 0.0;  
            for(j = 0; j < MODEL_NUM_TASKS; j++){
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
            p_temp[i] = NUM_TASKS_STATE + max_index;
            h_values[max_index] = -1.0;
        }
    }else if(policy == CANDIDATE1 || policy == CANDIDATE2){
        double min_val = 1e20;
        int min_index = 0;
        for(i = 0; i < MODEL_NUM_TASKS;i++){
            min_val = 1e20;  
            for(j = 0; j < MODEL_NUM_TASKS; j++){		
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
            p_temp[i] = NUM_TASKS_STATE + min_index;
            h_values[min_index] = 1e20;	
        }
    }
    
    for(i = 0; i < MODEL_NUM_TASKS;i++){
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
