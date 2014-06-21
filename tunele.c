#include <mpi.h>
#include <fcntl.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <time.h>
#define max(a, b) (((a) > (b)) ? (a) : (b))
#define min(a, b) (((a) > (b)) ? (b) : (a))
#define swap(x,y) {int tmp=x; x=y; y=tmp}

#define _4DEBUG
// #define _REALDEBUG
#define DEBUG_LEVEL 1
// #define DEBUG_TEST

#define QUEUE_SIZE 100
#define TRAVEL_TIME 30
#define KOSMODRON_WAIT 40

#define RAND_KOSMODRON_SPACE 10
#define RAND_ENERGY 1000
#define RAND_SLEEP_TIME 3

#define RESOURCES_NO 4
#define ENERGY 0
#define TUNNEL 1
#define DOCKPLACE_QUEUE 2

#define DOCKPLACE 3

#define REQUEST 5
#define REPLAY 6
#define RELEASE 7

struct queue_el {
    int resource_type;
    int clock;
    int source;
    int value;
};

struct resource_request {
    int clock;
    int ack_left;
    int value;
};

int size, rank, i;
char processor[100];
MPI_Status status;
int msg[3];

int clock_ = 1;
int planets, systems;
int dockplace_spaces;
time_t dockplace_timestamps[10];
int airfield_space, airfield_occupied, total_energy;
int airfield[RAND_KOSMODRON_SPACE];
struct queue_el queue[QUEUE_SIZE];
struct resource_request requests[RESOURCES_NO];

/* helper function */
int get_planet_no(int rank) {
    return rank % planets;
}

/* helper function */
int get_system_no(int rank) {
    return rank / systems;   
}

int get_tunel_no(int source, int destination) {
    source = get_system_no(source);
    destination = get_system_no(destination);

    return planets * max(source, destination) + min(source, destination);
}
 
int get_system_base(int rank) {
    return get_system_no(rank) * planets;
}

#define debug1(...) {if(DEBUG_LEVEL==1 || DEBUG_LEVEL==3){printf(__VA_ARGS__);}}
#define debug2(...) {if(DEBUG_LEVEL==2 || DEBUG_LEVEL==3){printf(__VA_ARGS__);}}
#ifdef _REALDEBUG
    #define debugR(...) {printf(__VA_ARGS__);}
#else
    #define debugR(...) {}
#endif

#ifdef _4DEBUG
    FILE *fp;
    char path[100] = "/dev/pts/";

    void init() {
        FILE *in;
        extern FILE *popen();
        char buff[512];

        if(!(in = popen("ls -al /dev/pts/ |grep inf106632 |rev |cut -d \" \" -f 1 |rev", "r"))){
            exit(1);
        }
        int pts = -1;

        while(fgets(buff, sizeof(buff), in)!=NULL) {
            pts++;
            // if (rank == 3) printf("buff: %d %d %s\n", pts, rank, buff);
            if (pts == rank) {
                // if (rank == 3) printf("=========================================\n");
                path[9] = buff[0];
                if (buff[1] != ' ' && buff[1] != '\n' && buff[1] != 0) {
                    path[10] = buff[1];
                } else {
                    path[10] = '\0';
                }
                path[11] = '\0';
            }
        }     
        pclose(in);

        // printf("%d: %s\n", rank, path);
        fp = fopen(path, "w");
        fprintf(fp, "\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n ==== PROCESS nr %d (energy %d) ====\n\n", rank, total_energy);
        if (fp == NULL) {
            printf("%d nie udało się podłączyć do zdalnej konsoli\n", rank);
            MPI_Finalize();
            exit(-1);
        }
    }

    #define printf(...) {fprintf(fp, "%d: ", clock_); fprintf(fp, __VA_ARGS__);}
#else
    // #define printf(...) {if (!rank) {printf("%d/%d @ %d: ", get_system_no(rank), get_planet_no(rank), clock_); printf(__VA_ARGS__);}}
    #ifndef _REALDEBUG
        #define printf(...) {{printf("%d/%d @ %d: ", get_system_no(rank), get_planet_no(rank), clock_); printf(__VA_ARGS__);}}
    #endif

#endif

void my_send(tag, resource_type, dest) {
    msg[0] = clock_;
    msg[1] = resource_type;
    if (dest != rank) {
        debug2("%s %d to %d\n", (tag == REPLAY) ? "REPLAY" : ((tag == REQUEST) ? "REQUEST" : "RELEASE"), resource_type, dest);
        debugR("%d %d %d %d %d %d\n", rank, dest, clock_, tag, resource_type, msg[2]);
        MPI_Send(msg, 3, MPI_INT, dest, tag, MPI_COMM_WORLD);
    }
}

/* helper function */
void queue_add(int resource_type, int clock_, int source, int value) {
    for (i = 0; i < QUEUE_SIZE; ++i) {
        if (!queue[i].clock) {
            queue[i].clock = clock_;
            queue[i].resource_type = resource_type;
            queue[i].source = source;
            queue[i].value = value;
            return;
        }
    }
}

void work() {
    MPI_Request request;
    int flag = 0;

    while (1) {
        MPI_Irecv(msg, 3, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &request);
        MPI_Test(&request, &flag, &status);
        if (flag) { 
             if (status.MPI_TAG == RELEASE) {
                if (msg[2] != -1) {
                    total_energy += msg[2];
                    debug2("ENERGY RELEASE from %d @%d\n", status.MPI_SOURCE, msg[0]);
                } else {
                    debug2("DOCKPLACE RELEASE from %d @%d\n", status.MPI_SOURCE, msg[0]);
                    int xx;
                    for (xx = 0; xx < 10; xx++) {
                        if (!dockplace_timestamps[xx]) {
                            time_t start;
                            time(&start);
                            dockplace_timestamps[xx] = start;
                            break;
                        }
                    }
                }
            } else if (status.MPI_TAG == REPLAY) {
                debug2("REPLAY %d from %d @%d\n", msg[1], status.MPI_SOURCE, msg[0]);   
                requests[msg[1]].ack_left--;
            } else {
                debug2("REQUEST %d from %d @%d\n", msg[1], status.MPI_SOURCE, msg[0]);   
                queue_add(msg[1], msg[0], status.MPI_SOURCE, msg[2]);
            }
            clock_ = max(clock_, msg[0]);
        } else {
            MPI_Cancel(&request);
            MPI_Request_free(&request);
            break;
        }
    }

    int xx;
    time_t end;
    time(&end);
    for (xx = 0; xx < 10; xx++) {
        if (dockplace_timestamps[xx] && (difftime(end, dockplace_timestamps[xx]) > KOSMODRON_WAIT)) {
            dockplace_timestamps[xx] = 0;
            dockplace_spaces++;
        }
    }


    int i;
    for (i = 0; i < QUEUE_SIZE; i++) {
        if (queue[i].clock) {
            // if (!rank) {
            //     printf("queue[%d] restype %d clocks %d %d vals %d %d  %d\n", i, queue[i].resource_type, requests[queue[i].resource_type].clock, queue[i].clock, queue[i].value, requests[queue[i].resource_type].clock, dockplace_spaces);
            // }
            msg[2] = queue[i].value;
            if ((queue[i].resource_type != DOCKPLACE) && (
                requests[queue[i].resource_type].clock == 0 || 
                requests[queue[i].resource_type].clock > queue[i].clock ||
                (requests[queue[i].resource_type].clock == queue[i].clock && queue[i].source < rank) ||
                (queue[i].resource_type == TUNNEL && requests[TUNNEL].value != queue[i].value) ||
                (queue[i].resource_type == DOCKPLACE_QUEUE && requests[DOCKPLACE_QUEUE].value != queue[i].value)
            )) {
                my_send(REPLAY, queue[i].resource_type, queue[i].source);
                if (queue[i].resource_type == ENERGY) {
                    total_energy -= queue[i].value;
                }
                queue[i].clock = 0;
            } else if ((queue[i].resource_type == DOCKPLACE) && (dockplace_spaces > 0)) {
                my_send(REPLAY, queue[i].resource_type, queue[i].source);
                dockplace_spaces--;
                queue[i].clock = 0;
            }

        }
    }
}

void my_idle(int seconds) {
    time_t start, end;

    time(&start);
    do {
        work();
        time(&end);
    } while (difftime(end, start) < seconds);

}

void my_wait() {
    int i;
    for (i = 0; i < RESOURCES_NO; ++i) {
        while (requests[i].ack_left) {
            work();
        }
    }
}

void run()
{
    #ifdef _4DEBUG
        init();
    #endif

    airfield_space = rand() % RAND_KOSMODRON_SPACE;
    airfield_occupied = 0;

    int energy, destination, sleep_time;
    while (1) {
        // if (rank) {
        //     while(1) {
        //         work();
        //     }
        // }

        /* rand outgoing ship */
        energy = rand() % RAND_ENERGY + RAND_ENERGY / 5;
        sleep_time = rand() % RAND_SLEEP_TIME + 1;
        do {
            destination = rand() % (systems * planets);
            // work();
        }
        while (get_system_no(destination) == get_system_no(rank));
        
        /* wait before starting */
        // my_idle(sleep_time);

        // printf("%d %d %d %d %d %d\n", total_energy, airfield_space, airfield_occupied, energy, destination, sleep_time);
        debug1("-- NEW SHIP to %d, energy = %d\n", destination, energy);
        #ifdef DEBUG_TEST
            printf("new ship to %d energy %d tunnel %d\n", destination, energy, get_tunel_no(rank, destination));
        #endif
        debug1("requesting %d DOCKPLACE_QUEUE...\n", destination);
        clock_++;
        msg[2] = destination;
        requests[DOCKPLACE_QUEUE].clock = clock_;
        requests[DOCKPLACE_QUEUE].ack_left = planets * systems - 1;
        requests[DOCKPLACE_QUEUE].value = destination;
        for (i = 0; i < systems * planets; ++i) {
            my_send(REQUEST, DOCKPLACE_QUEUE, i);
        }
        my_wait();
        debug1("requesting %d DOCKPLACE_QUEUE... DONE\n", destination);

        debug1("requesting %d DOCKPLACE...\n", destination);
        clock_++;
        msg[2] = destination;
        requests[DOCKPLACE].clock = clock_;
        requests[DOCKPLACE].ack_left = 1;
        requests[DOCKPLACE].value = destination;
        my_send(REQUEST, DOCKPLACE, destination);
        my_wait();
        debug1("requesting %d DOCKPLACE... DONE\n", destination);

        debug1("releasing DOCKPLACE_QUEUE...\n");
        requests[DOCKPLACE_QUEUE].clock = 0;
        work();
        debug1("releasing DOCKPLACE_QUEUE... DONE\n");

        debug1("requesting %d ENERGY...\n", energy);
        clock_++;
        msg[2] = energy;
        requests[ENERGY].clock = clock_;
        requests[ENERGY].ack_left = planets * systems - 1;
        for (i = 0; i < systems * planets; ++i) {
            my_send(REQUEST, ENERGY, i);
        }

        my_wait();
        while (total_energy < energy) {
            work();
        }
        // release energy queue
        requests[ENERGY].clock = 0;
        work();
        debug1("requesting %d ENERGY... DONE\n", energy);

        debug1("requesting %d TUNNEL...\n", get_tunel_no(rank, destination));
        clock_++;
        msg[2] = get_tunel_no(rank, destination);
        requests[TUNNEL].clock = clock_;
        requests[TUNNEL].ack_left = 2 * planets - 1;
        requests[TUNNEL].value = get_tunel_no(rank, destination);
        // debug1("requests[%d] left %d\n", TUNNEL, requests[TUNNEL].ack_left);
        for (i = 0; i < planets; ++i) {
            my_send(REQUEST, TUNNEL, get_system_base(destination)+ i);
            my_send(REQUEST, TUNNEL, get_system_base(rank) + i); 
        }
        my_wait(); 
        debug1("requesting %d TUNNEL... DONE\n", get_tunel_no(rank, destination));

        #ifdef DEBUG_TEST
            printf("traveling...\n");
        #endif

        debug1("-- TRAVELING...\n");
        my_idle(TRAVEL_TIME);
        debug1("-- TRAVELING... DONE\n");

        debug1("releasing TUNNEL...\n")
        requests[TUNNEL].clock = 0;
        work();
        debug1("releasing TUNNEL... DONE\n")

        debug1("releasing ENERGY...\n")
        msg[2] = energy;
        for (i = 0; i < systems * planets; ++i) {
            my_send(RELEASE, ENERGY, i);
        }
        debug1("releasing ENERGY... DONE\n")
        
        debug1("releasing DOCKPLACE...\n");
        msg[2] = -1;
        my_send(RELEASE, DOCKPLACE, destination);
        debug1("releasing DOCKPLACE... DONE\n");
    }
}

int main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Get_processor_name(processor, &i);

    if (argc < 2) {
        if (!rank) {
            printf("Usage: ./nazwa [liczba ukladow] [liczba planet]\n");
        }
        MPI_Finalize();
        exit(-1);
    }

    systems = atoi(argv[1]);
    planets = atoi(argv[1]);
    // total_energy = RAND_ENERGY * planets * systems * RAND_KOSMODRON_SPACE / 4;
    total_energy = RAND_ENERGY + (RAND_ENERGY / 2) * (planets * (planets - 1) / 2);
    dockplace_spaces = 1;

    if (systems * planets > rank) {
        srand(time(NULL) + rank);
        run();
    }

    MPI_Finalize();
    return 0;
}
