#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <stdbool.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

//Initialization======================================
#define MAX_PROCESSES 64

//Process state
typedef enum {
    STATE_NONE = 0,     
    STATE_RUNNING,      
    STATE_BLOCKED,     
    STATE_ZOMBIE,       
    STATE_TERMINATED    
} ProcessState;

// PCB Class. Eitar object hobe process gula
typedef struct {
    int pid;           
    int ppid;          
    ProcessState state; 
    int exit_status;

    int children[MAX_PROCESSES]; 
    int num_children;
    
    //pthread_mutex_t pcb_mutex;
    pthread_cond_t pcb_cond; // individual thread sleep and wakeup variable

} PCB;

PCB process_table[MAX_PROCESSES];   //Main Table
int next_available_pid = 1;  //PID generator variable
//Initializing mutex and monitor thread
pthread_mutex_t table_mutex = PTHREAD_MUTEX_INITIALIZER; //Only one thread will modify the table at a time
pthread_cond_t monitor_cond = PTHREAD_COND_INITIALIZER; //Monitoring thread will use this variable to wake up and check if something has updated in the table when another thread sends a signal

pthread_t monitor_thread;
bool system_running = true;


void init_system() {

    pthread_mutex_lock(&table_mutex); //locking table

    // Initializing empty table
    for (int i = 0; i < MAX_PROCESSES; i++) {
        process_table[i].state = STATE_NONE;
        process_table[i].num_children = 0;
        //pthread_mutex_init(&process_table[i].pcb_mutex, NULL);
        pthread_cond_init(&process_table[i].pcb_cond, NULL);
    }

    process_table[0].pid = next_available_pid++;
    process_table[0].ppid = 0;
    process_table[0].state = STATE_RUNNING;
    process_table[0].exit_status = -1; 
    process_table[0].num_children = 0;

    pthread_mutex_unlock(&table_mutex);
}


// MONITOR THREAD ====================================================== 

void* monitor_loop(void* arg) {
    FILE* log_file = fopen("snapshots.txt", "a");
    if (log_file == NULL) {
        perror("Failed to open snapshots.txt");
        return NULL;
    }

    while (system_running) {
        pthread_mutex_lock(&table_mutex);
        
        pthread_cond_wait(&monitor_cond, &table_mutex);

        
        fprintf(log_file, "\n=== Snapshot at %ld ===\n", time(NULL));
        fprintf(log_file, "%-8s %-8s %-12s %-12s\n", "PID", "PPID", "STATE", "EXIT_STATUS");
        fprintf(log_file, "----------------------------------------------\n");

        for (int i = 0; i < MAX_PROCESSES; i++) {
            if (process_table[i].state != STATE_NONE && 
                process_table[i].state != STATE_TERMINATED) {
                
                char* state_str;
                switch (process_table[i].state) {
                    case STATE_RUNNING:  state_str = "RUNNING";  break;
                    case STATE_BLOCKED:  state_str = "BLOCKED";  break;
                    case STATE_ZOMBIE:   state_str = "ZOMBIE";   break;
                    default:             state_str = "UNKNOWN";  break;
                }

                if (process_table[i].state == STATE_ZOMBIE) {
                    fprintf(log_file, "%-8d %-8d %-12s %-12d\n",
                            process_table[i].pid, 
                            process_table[i].ppid, 
                            state_str, 
                            process_table[i].exit_status);
                } else {
                    fprintf(log_file, "%-8d %-8d %-12s %-12s\n",
                            process_table[i].pid, 
                            process_table[i].ppid, 
                            state_str, 
                            "-");
                }
            }
        }
        fprintf(log_file, "\n");
        fflush(log_file);        

        pthread_mutex_unlock(&table_mutex);
    }

    fclose(log_file);
    return NULL;
}

// Helper to notify the monitor thread of changes 
void notify_monitor() {
    pthread_cond_signal(&monitor_cond);
}

void start_monitor_thread() {
    system_running = true;
    if (pthread_create(&monitor_thread, NULL, monitor_loop, NULL) != 0) {
        perror("Failed to create monitor thread");
    }
}

void stop_monitor_thread() {
    pthread_mutex_lock(&table_mutex);     // 1. Lock the table
    system_running = false;               // 2. Change the flag
    pthread_cond_signal(&monitor_cond);   // 3. Send the wake-up call
    pthread_mutex_unlock(&table_mutex);   // 4. Unlock
    
    pthread_join(monitor_thread, NULL);   // Wait for it to finish
}
//Snapshot=======================================================================
void pm_ps() {
    printf("\n%-8s %-8s %-12s %-12s\n", "PID", "PPID", "STATE", "EXIT_STATUS");
    printf("----------------------------------------------\n");
    for (int i = 0; i < MAX_PROCESSES; i++) {
        if (process_table[i].state != STATE_NONE && process_table[i].state != STATE_TERMINATED) { //empty slots dekhabo na
            char* state_str;
            switch (process_table[i].state) {
                case STATE_RUNNING: state_str = "RUNNING"; break;
                case STATE_BLOCKED: state_str = "BLOCKED"; break;
                case STATE_ZOMBIE:  state_str = "ZOMBIE"; break;
                default:            state_str = "UNKNOWN"; break;
            }
            
            if (process_table[i].state == STATE_ZOMBIE) {
                printf("%-8d %-8d %-12s %-12d\n", 
                       process_table[i].pid, process_table[i].ppid, state_str, process_table[i].exit_status);
            } else {
                printf("%-8d %-8d %-12s %-12s\n", 
                       process_table[i].pid, process_table[i].ppid, state_str, "-");
            }
        }
    }
    printf("\n");
}


//Operations on Process========================================

int pm_fork(int parent_pid) {
    pthread_mutex_lock(&table_mutex);
    
    int slot = -1; // if initialized with 0, it might take over process at slot 0
    for (int i = 0; i < MAX_PROCESSES; i++) {
        if (process_table[i].state == STATE_NONE) {
            slot = i; // found empty slot
            break;
        }
    }

    if (slot == -1) {
        pthread_mutex_unlock(&table_mutex);
        return -1; // Table full, no slot
    }

    // initialize the process at the empty slot found
    process_table[slot].pid = next_available_pid++;
    process_table[slot].ppid = parent_pid;
    process_table[slot].state = STATE_RUNNING;
    process_table[slot].exit_status = -1;
    process_table[slot].num_children = 0;

    // Link child to parent
    for (int i = 0; i < MAX_PROCESSES; i++) {
        if (process_table[i].pid == parent_pid) {
            process_table[i].children[process_table[i].num_children++] = process_table[slot].pid; //put pid of child at parent's children array
            break;
        }
    }

    int new_pid = process_table[slot].pid;
    notify_monitor();
    pthread_mutex_unlock(&table_mutex);
    return new_pid;
}


void pm_exit(int pid, int status) {
    pthread_mutex_lock(&table_mutex);
    
    int parent_pid = -1;
    for (int i = 0; i < MAX_PROCESSES; i++) { //finds the process to be terminated and stores it's parent
        if (process_table[i].pid == pid) {
            process_table[i].state = STATE_ZOMBIE;
            process_table[i].exit_status = status;
            parent_pid = process_table[i].ppid;
            break;
        }
    }

    // Wake up the parent if it is waiting //finds the parent in the process table found from previous loop
    for (int i = 0; i < MAX_PROCESSES; i++) { //if parent_pid == -1 still. this loop won't find the parent
        if (process_table[i].pid == parent_pid) {
            pthread_cond_signal(&process_table[i].pcb_cond); //parent wakeup
            break;
        }
    }

    notify_monitor();
    pthread_mutex_unlock(&table_mutex);
}

// Parent waits for a specific child or any child
int pm_wait(int parent_pid, int child_pid) {
    pthread_mutex_lock(&table_mutex);
    
    PCB* parent = NULL;
    int parent_idx = -1;
    for (int i = 0; i < MAX_PROCESSES; i++) {
        if (process_table[i].pid == parent_pid) {
            parent = &process_table[i];
            parent_idx = i;
            break;
        }
    }
    
    // If no child---> return trivially 
    if (parent == NULL || parent->num_children == 0) {
        pthread_mutex_unlock(&table_mutex);
        return -1; 
    }

    while (1) {
        int found_zombie_idx = -1;
        for (int i = 0; i < MAX_PROCESSES; i++) {
            if (process_table[i].ppid == parent_pid && process_table[i].state == STATE_ZOMBIE) { // if the process is zombie
                if (child_pid == -1 || process_table[i].pid == child_pid) { // any child or specific child
                    found_zombie_idx = i;
                    break;
                }
            }
        }

        if (found_zombie_idx != -1) { // child found. Therefore terminate it
            int status = process_table[found_zombie_idx].exit_status;
            process_table[found_zombie_idx].state = STATE_TERMINATED;
            notify_monitor();
            pthread_mutex_unlock(&table_mutex);
            return status;
        }

        // no child found. Therefore parent waits again
        parent->state = STATE_BLOCKED;
        notify_monitor(); // 1. Notify monitor that parent is now BLOCKED

        pthread_cond_wait(&parent->pcb_cond, &table_mutex);
        
        // 2. Drop the lock for a tiny fraction of a second so the monitor 
        // can print the child's ZOMBIE state before we terminate it.
        pthread_mutex_unlock(&table_mutex);
        usleep(1000); 
        pthread_mutex_lock(&table_mutex);

        parent->state = STATE_RUNNING;
        notify_monitor(); // 3. Notify monitor that parent is now RUNNING again
    }
}


void pm_kill(int pid) {
    pm_exit(pid, 0);
}


//
// dcript interpreter and workerthreads========================

//  worker threads class
typedef struct {
    int thread_id;
    char filename[256];
} WorkerArgs;


void* worker_routine(void* arg) {
    WorkerArgs* args = (WorkerArgs*)arg;
    
    // 1 worker gets 1 file
    FILE* file = fopen(args->filename, "r");
    if (file == NULL) {
        fprintf(stderr, "Thread %d: Failed to open script file %s\n", args->thread_id, args->filename);
        free(args);
        return NULL;
    }

    char line[256];
    // File read korbe and execute
    while (fgets(line, sizeof(line), file)) {
        char command[16];
        int arg1 = 0, arg2 = 0;
        
        int parsed = sscanf(line, "%15s %d %d", command, &arg1, &arg2);
        if (parsed < 1) continue; // Skip blank lines

        if (strcmp(command, "fork") == 0) {
            pm_fork(arg1);
        } 
        else if (strcmp(command, "exit") == 0) {
            pm_exit(arg1, arg2);
        } 
        else if (strcmp(command, "wait") == 0) {
            pm_wait(arg1, arg2);
        } 
        else if (strcmp(command, "kill") == 0) {
            pm_kill(arg1);
        } 
        else if (strcmp(command, "sleep") == 0) {
            
            struct timespec ts;
            ts.tv_sec = arg1 / 1000;
            ts.tv_nsec = (arg1 % 1000) * 1000000;
            nanosleep(&ts, NULL);
        }
    }

    fclose(file);
    free(args);
    return NULL;
}

// ================================Main Program============

int main(int argc, char *argv[]) {

    if (argc < 2) {
        printf("Usage: %s <script1.txt> [script2.txt] ...\n", argv[0]);
        return 1;
    }

    // number of worker threads== number of script names passed 
    int num_threads = argc - 1;
    pthread_t* workers = malloc(num_threads * sizeof(pthread_t));

    init_system();
    start_monitor_thread();

    // creating worker to deal with the scripts
    for (int i = 0; i < num_threads; i++) {
        WorkerArgs* args = malloc(sizeof(WorkerArgs));
        
        args->thread_id = i; 
        
        strncpy(args->filename, argv[i + 1], sizeof(args->filename) - 1);
        args->filename[sizeof(args->filename) - 1] = '\0';
        
        if (pthread_create(&workers[i], NULL, worker_routine, args) != 0) {
            perror("Failed to create worker thread");
        }
    }

    // kaaj sesh koruk
    for (int i = 0; i < num_threads; i++) {
        pthread_join(workers[i], NULL);
    }

    stop_monitor_thread();
    free(workers);

    printf("Simulation complete. Check snapshots.txt for logs.\n");

    return 0;
}