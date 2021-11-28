#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <json-c/json.h>
#include <string.h>
#include <math.h>
#include <time.h>
#define N 3505
#define numberOfProcessors 3
#define historyOfProcessor 4
#define ll long long int

struct task
{
    int taskID;
    int executionTime;
    int taskType;
    struct task *next;
};

struct dependency
{ //"afterID" can be executed only after finishing "beforeID"
    int beforeID;
    int afterID;
    int transferTime;
    struct dependency *next;
};

struct DAG
{
    int dagID;
    int dagType;
    int arrivalTime;
    int deadlineTime;
    struct task *listOfTasks;              // list of all tasks (just their IDs and execution times, order doesn't matter) of DAG
    struct dependency *listOfDependencies; // all edges (dependencies) of DAG (NULL if there are no dependencies)
    struct task *lastTask;
    struct dependency *lastDependency;
    struct task *firstTask;
    struct dependency *firstDependency;
};

struct DAG *input[N];
int dagsCount = 0; // total number of DAGs (0 indexed in array "input")

void initialize(int whichDag)
{
    input[whichDag] = malloc(sizeof(struct DAG));
    input[whichDag]->listOfTasks = NULL;
    input[whichDag]->listOfDependencies = NULL;
    input[whichDag]->lastDependency = NULL;
    input[whichDag]->lastTask = NULL;
    input[whichDag]->firstDependency = NULL;
    input[whichDag]->firstTask = NULL;
}

void add_task_to_list(int whichDag, int taskID, int executionTime, int taskType)
{
    if (input[whichDag]->lastTask == NULL)
    {
        input[whichDag]->listOfTasks = malloc(sizeof(struct task));
        input[whichDag]->lastTask = input[whichDag]->listOfTasks;
        input[whichDag]->firstTask = input[whichDag]->lastTask;
    }
    else
    {
        input[whichDag]->lastTask->next = malloc(sizeof(struct task));
        input[whichDag]->lastTask = input[whichDag]->lastTask->next;
    }
    input[whichDag]->lastTask->taskID = taskID;
    input[whichDag]->lastTask->executionTime = executionTime;
    input[whichDag]->lastTask->taskType = taskType;
    input[whichDag]->lastTask->next = NULL;
    return;
}

void add_dependency_to_list(int whichDag, int beforeID, int afterID, int transferTime)
{
    if (input[whichDag]->lastDependency == NULL)
    {
        input[whichDag]->listOfDependencies = malloc(sizeof(struct dependency));
        input[whichDag]->lastDependency = input[whichDag]->listOfDependencies;
        input[whichDag]->firstDependency = input[whichDag]->lastDependency;
    }
    else
    {
        input[whichDag]->lastDependency->next = malloc(sizeof(struct dependency));
        input[whichDag]->lastDependency = input[whichDag]->lastDependency->next;
    }
    input[whichDag]->lastDependency->beforeID = beforeID;
    input[whichDag]->lastDependency->afterID = afterID;
    input[whichDag]->lastDependency->transferTime = transferTime;
    input[whichDag]->lastDependency->next = NULL;
    return;
}

void print_dag_tasks(int whichDag)
{ // "whichDag" is index of DAG in array "input"
    struct task *current = input[whichDag]->firstTask;
    while (current != NULL)
    {
        printf("%d ", current->taskID);
        current = current->next;
    }
    printf("\n");
}

void print_dag_dependencies(int whichDag)
{ // "whichDag" is index of DAG in array "input"
    struct dependency *current = input[whichDag]->firstDependency;
    while (current != NULL)
    {
        printf("FROM: %d TO: %d COST: %d\n", current->beforeID, current->afterID, current->transferTime);
        current = current->next;
    }
    printf("\n");
}

void reader_function(char *filename)
{ // This function reads input from the json file
    FILE *fp;
    char buffer[1024 * 1024];
    fp = fopen(filename, "r");
    fread(buffer, sizeof(char), 1024 * 1024, fp);
    fclose(fp);

    // get DAGs descriptions one by one, then parse it into struct

    struct json_object *parsedJson = json_tokener_parse(buffer);
    json_object_object_foreach(parsedJson, key, val)
    {
        enum json_type type;
        type = json_object_get_type(val);
        if (type == json_type_object)
        {
            initialize(dagsCount);
            json_object_object_foreach(val, keyR, valR)
            {
                enum json_type typeR;
                typeR = json_object_get_type(valR);
                if (typeR == json_type_object)
                {
                    char buffer[20];
                    int taskID = atoi(strncpy(buffer, &keyR[4], 20)); // this method takes a string slices then parses it to int
                    //
                    // example 1, 2, 3, 4, 0001 yes but wait a sec
                    struct json_object *executionTime, *taskType;
                    json_object_object_get_ex(valR, "EET", &executionTime);
                    json_object_object_get_ex(valR, "Type", &taskType);
                    int exeTime = json_object_get_int(executionTime);
                    int type = json_object_get_int(taskType);
                    add_task_to_list(dagsCount, taskID, exeTime, type);
                    struct json_object *edges;
                    bool exists = json_object_object_get_ex(valR, "next", &edges);
                    if (exists)
                    {
                        json_object_object_foreach(edges, taskTo, time)
                        {
                            int transferTime = json_object_get_int(time);
                            char buffer[20];
                            int afterID = atoi(strncpy(buffer, &taskTo[4], 20));
                            add_dependency_to_list(dagsCount, taskID, afterID, transferTime);
                        }
                    }
                }
            }
            struct json_object *type, *arrivalTime, *relativeDeadline;
            json_object_object_get_ex(val, "Type", &type);
            json_object_object_get_ex(val, "ArrivalTime", &arrivalTime);
            json_object_object_get_ex(val, "Deadline", &relativeDeadline);
            input[dagsCount]->dagType = json_object_get_int(type);
            input[dagsCount]->arrivalTime = json_object_get_int(arrivalTime);
            input[dagsCount]->deadlineTime = json_object_get_int(relativeDeadline) + input[dagsCount]->arrivalTime;
            char buffer[20];
            input[dagsCount]->dagID = atoi(strncpy(buffer, &key[3], 20));
            dagsCount++;
        }
    }
}

struct ProcessorSchedule
{
    int numberOfTasks; // place number of tasks scheduled for this processor here
    int taskIDs[N];    // place IDs of tasks scheduled for this processor here, in order of their execution (start filling from taskIDs[0])
    int startTime[N];  // place starting times of scheduled tasks, so startTime[i] is start time of task with ID taskIDs[i] (all should be integers) (0 indexed)
    int exeTime[N];    // place actual execution times of tasks, so exeTime[i] is execution time of i-th task scheduled on this processor (0 indexed)
};

struct ProcessorSchedule output[numberOfProcessors]; // our processors

// auxiliary structures for output function (you dont have to read them)
struct TaskWithDeadline
{
    int taskId;
    int dagDeadline;
};

struct TaskWithFinishTime
{
    int taskId;
    int finishTime;
};

int cmp_aux(const void *arg11, const void *arg22)
{
    struct TaskWithDeadline *arg1 = (struct TaskWithDeadline *)arg11;
    struct TaskWithDeadline *arg2 = (struct TaskWithDeadline *)arg22;
    if ((arg1->taskId) < (arg2->taskId))
        return -1;
    if ((arg1->taskId) == (arg2->taskId))
        return 0;
    return 1;
}

int cmp_aux2(const void *arg11, const void *arg22)
{
    struct TaskWithFinishTime *arg1 = (struct TaskWithFinishTime *)arg11;
    struct TaskWithFinishTime *arg2 = (struct TaskWithFinishTime *)arg22;
    if ((arg1->taskId) < (arg2->taskId))
        return -1;
    if ((arg1->taskId) == (arg2->taskId))
        return 0;
    return 1;
}

clock_t begin, end;

void printer_function(char *filename)
{
    // call this function once output table is filled, it will automaticly write data to file in correct format
    FILE *f = fopen(filename, "w");
    for (int i = 0; i < numberOfProcessors; i++)
    {
        for (int j = 0; j < output[i].numberOfTasks; j++)
        {
            //fprintf(f, "%d %d %d,", output[i].taskIDs[j], output[i].startTime[j], output[i].startTime[j] + output[i].exeTime[j]);
            fprintf(f, "output[%d][j->%d] -> taskIDs[j]: %d startTime[j]: %d startTime[j] + exeTime[j]: %d,", i, j, output[i].taskIDs[j], output[i].startTime[j], output[i].startTime[j] + output[i].exeTime[j]);
        }
        fprintf(f, "\n");
    }
    int taskNumber = 0;
    double worstMakespan = 0;
    for (int i = 0; i < dagsCount; i++)
    {
        struct task *current = input[i]->firstTask;
        while (current != NULL)
        {
            taskNumber += 1;
            worstMakespan += current->executionTime;
            current = current->next;
        }
        struct dependency *dep = input[i]->firstDependency;
        while (dep != NULL)
        {
            worstMakespan += dep->transferTime;
            dep = dep->next;
        }
    }
    ll makespan = 0;
    for (int i = 0; i < numberOfProcessors; i++)
    {
        int count = output[i].numberOfTasks;
        if (count == 0)
            continue;
        if (output[i].startTime[count - 1] + output[i].exeTime[count - 1] > makespan)
            makespan = output[i].startTime[count - 1] + output[i].exeTime[count - 1];
    }
    //fprintf(f, "%lld\n", makespan);
    fprintf(f, "makespan: %lld\n", makespan);

    double sumOfSquares = 0;
    double sum = 0;
    for (int i = 0; i < numberOfProcessors; i++)
    {
        double length = 0;
        for (int j = 0; j < output[i].numberOfTasks; j++)
            length += output[i].exeTime[j];
        sumOfSquares += (double)(length * 1.0 / makespan) * (double)(length * 1.0 / makespan);
        sum += length / makespan;
    }
    sumOfSquares /= numberOfProcessors;
    sum /= numberOfProcessors;
    sum *= sum;
    sumOfSquares -= sum;
    double stdev = sqrt(sumOfSquares);
    // fprintf(f, "%0.6lf\n", stdev);
    fprintf(f, "stdev: %0.6lf\n", stdev);

    struct TaskWithDeadline table1[N];
    struct TaskWithFinishTime table2[N];
    int done = 0;
    for (int i = 0; i < dagsCount; i++)
    {
        struct task *now = input[i]->listOfTasks;
        while (now != NULL)
        {
            struct TaskWithDeadline current;
            current.taskId = now->taskID;
            current.dagDeadline = input[i]->deadlineTime;
            table1[done++] = current;
            now = now->next;
        }
    }
    done = 0;
    for (int i = 0; i < numberOfProcessors; i++)
    {
        for (int j = 0; j < output[i].numberOfTasks; j++)
        {
            struct TaskWithFinishTime current;
            current.taskId = output[i].taskIDs[j];
            current.finishTime = output[i].startTime[j] + output[i].exeTime[j];
            table2[done++] = current;
        }
    }

    qsort(table1, taskNumber, sizeof(struct TaskWithDeadline), cmp_aux);
    qsort(table2, taskNumber, sizeof(struct TaskWithFinishTime), cmp_aux2);

    int missedDeadlines = 0;
    for (int i = 0; i < taskNumber; i++)
    {
        if (table1[i].dagDeadline < table2[i].finishTime)
            missedDeadlines++;
    }

    double costFunction = (double)makespan / worstMakespan * 10 + stdev;
    costFunction = 1 / costFunction;
    //fprintf(f, "%0.3lf\n", costFunction);
    fprintf(f, "costFunction: %0.3lf\n", costFunction);

    double time_spent = (double)(end - begin) / CLOCKS_PER_SEC;
    int spent = (int)(time_spent * 1000);
    //fprintf(f, "%d\n", spent);
    fprintf(f, "spent: %d\n", spent);
    fclose(f);
}

int inDegree[N];                          // auxiliary array ONLY for sample scheduler
int topsortOrder[N];                      // auxiliary array ONLY for sample scheduler
int earliestStart[numberOfProcessors][N]; // auxiliary array ONLY for sample scheduler
int executionTime[N];                     // auxiliary array ONLY for sample scheduler
int taskType[N];                          // auxiliary array ONLY for sample scheduler

int schedule_task(int procID, int earliestPossibleStart, int taskID, int exeTime)
{
    // schedule task at processing node, after previously scheduled tasks
    int lastFinish = 0;
    int taskCount = output[procID].numberOfTasks;
    if (taskCount > 0)
        lastFinish = output[procID].startTime[taskCount - 1] + output[procID].exeTime[taskCount - 1];
    if (earliestPossibleStart > lastFinish)
        lastFinish = earliestPossibleStart;

    // check if task of the same was in the nearest past
    int history = 0;
    bool cache = false;
    for (int j = taskCount - 1; j >= 0; j--)
    {
        if (taskType[output[procID].taskIDs[j]] == taskType[taskID])
            cache = true;
        history++;
        if (history == historyOfProcessor)
            break;
    }
    if (cache)
    {
        // possibly 10% shorter execution time
        exeTime *= 9;
        exeTime /= 10;
    }
    output[procID].startTime[taskCount] = lastFinish;
    output[procID].exeTime[taskCount] = exeTime;
    output[procID].taskIDs[taskCount] = taskID;
    output[procID].numberOfTasks++;
    return lastFinish + exeTime;
}

void initialize_processors()
{
    for (int i = 0; i < numberOfProcessors; i++)
    {
        output[i].numberOfTasks = 0;
    }
}

void schedule_dag(int id)
{
    // schedule all tasks from input[id]
    // NOTE: ONLY IN THE SAMPLE TEST we assume that task ids are smaller than N (for simplicity of sample solution) which is NOT TRUE for general case!
    struct dependency *current = input[id]->firstDependency;
    while (current != NULL)
    {
        // int from = current->beforeID;
        int to = current->afterID;
        inDegree[to]++;
        current = current->next;
    } // populate all dependices of this dag

    int topsortSize = 0;
    int topsortPtr = 0;
    struct task *ptr = input[id]->firstTask;
    int arrivalTime = input[id]->arrivalTime; // dag arrival time
    // initialize topsort array
    while (ptr != NULL)
    {
        int id = ptr->taskID;
        if (inDegree[id] == 0) // if indegree for this task is 0
        {
            // Store the id, increment array index
            topsortOrder[topsortSize++] = id; // aux array
        }
        executionTime[id] = ptr->executionTime; // get exeTime
        taskType[id] = ptr->taskType; // get type of task
        for (int i = 0; i < numberOfProcessors; i++) // loop for procs
        {
            // [Number of process][taskID]
            earliestStart[i][id] = arrivalTime; 
        }
        // go to next
        ptr = ptr->next;
    }
    // add all tasks to topsort array
    while (topsortPtr < topsortSize)
    {
        // Get current working id stored from previous while loop
        int currentID = topsortOrder[topsortPtr++];
        // Get first dependency for DAG
        struct dependency *list = input[id]->firstDependency;
        // For some fucked up reason they refer to dependency
        // as 'list'
        while (list != NULL)
        {
            int from = list->beforeID;
            int to = list->afterID;
            if (currentID == from)
            {
                inDegree[to]--;
                if (inDegree[to] == 0)
                {
                    topsortOrder[topsortSize++] = to;
                }
            }
            list = list->next;
        }
    }

    // topsort[0 .... topsortSize - 1] is list of task IDs sorted in topological order
    for (int i = 0; i < topsortSize; i++)
    {
        // cyclically pick a processor
        // Round-robin
        int procChoice = i % numberOfProcessors;
        // struct ProcessorSchedule
        // {
        //     int numberOfTasks; // place number of tasks scheduled for this processor here
        //     int taskIDs[N];    // place IDs of tasks scheduled for this processor here, in order of their execution (start filling from taskIDs[0])
        //     int startTime[N];  // place starting times of scheduled tasks, so startTime[i] is start time of task with ID taskIDs[i] (all should be integers) (0 indexed)
        //     int exeTime[N];    // place actual execution times of tasks, so exeTime[i] is execution time of i-th task scheduled on this processor (0 indexed)
        // };
        int currentID = topsortOrder[i]; //tasks to be scheduled
        // int taskLimit = 4;
        // if (output[procChoice].numberOfTasks < taskLimit) {
        //     for (int procID = 0; procID < numberOfProcessors; procID++)
        //     {
        //         int numScheduled = 0;
        //         for (int task = 0; task < output[procID].numberOfTasks; task++)
        //         {
        //             int scheduledTask = output[procID].taskIDs[task]; //scheduled tasks
        //             if (currentID == scheduledTask) {
        //                 procChoice = procID;
        //                 break;
        //             }
        //         }
        //     }
        // }

        // // increases spent
        // for (int goDeeper = 0; goDeeper < 1000000; goDeeper++){
        //     continue;
        // }

        // for(int z = 0; z < output[i].numberOfTasks; z++) {
        //     printf("%d, ", output[i].taskIDs[z]);
        // }
        //printf("procChoice[%d] -> nTasks:[%d] \n", i, output[i].numberOfTasks);

        //int currentID = topsortOrder[i];
        // we schedule tasks on the processors cyclically
        // original
        // int finish = schedule_task(i % numberOfProcessors, earliestStart[i % numberOfProcessors][currentID], currentID, executionTime[currentID]);
        int finish = schedule_task(procChoice, earliestStart[i % numberOfProcessors][currentID], currentID, executionTime[currentID]);

        // look at later
        struct dependency *list = input[id]->firstDependency;
        while (list != NULL)
        {
            int from = list->beforeID;
            int to = list->afterID;
            if (from == currentID)
            {
                for (int proc = 0; proc < numberOfProcessors; proc++)
                {
                    int bestTime = finish + list->transferTime * (proc != (i % numberOfProcessors));
                    if (bestTime > earliestStart[proc][to])
                    {
                        earliestStart[proc][to] = bestTime;
                    }
                }
            }
            list = list->next;
        }
    }
    // ### ADDED ###
    // for (int i = 0; i < numberOfProcessors; i++)
    // {
    //     printf("procID: %d -> ", i);
    //     int current = output[i].numberOfTasks;
    //     printf("%d \n", current);
    // }
}

void printDag(int idx)
{
    struct DAG *current = input[idx];
    printf("dagID[%d]->", current->dagID);
    printf("[type:%d, ", current->dagType);
    printf("arrival: %d, ", current->arrivalTime);
    printf("deadline: %d, ", current->deadlineTime);
    printf("firstTask: [id:%d, type:%d, exeTime:%d]",
           current->firstTask->taskID,
           current->firstTask->taskType,
           current->firstTask->executionTime);
    printf("]\n");
}

void scheduler()
{ // fill this function, you have dags in input array, and you need to schedule them to processors in output array   
     for (int i = 0; i < dagsCount; i++)
    {
        /* IDEAS
            - you can maybe manipulate the dag before you send it for scheduling?
            - probably dont loop through dags but instead build a que of dags in a smart way then deque to schedule
            - load balancing
        */
        // schedule dags one by one
        // our current dag

        // scheduling via the current i: int

        schedule_dag(i);
    }
    // for (int i = 0; i < dagsCount; i++) {
    //     int current = input[i]->dagType;
    //     printf("%d \n", current);
    //     // schedule_dag(1);
    // }
    // schedule_dag(0);
}

int main(int argc, char *argv[])
{
    // fill output table[0 - 7] (8 processors), in each structure put IDs of scheduled tasks as described above
    reader_function(argv[1]);
    initialize_processors();
    begin = clock();
    scheduler(); // fill this
    end = clock();
    printer_function(argv[2]);
    return 0;
}
