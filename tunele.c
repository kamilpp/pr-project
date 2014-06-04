#include <mpi.h>
#include <fcntl.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <time.h>
#define max(a, b) (((a) > (b)) ? (a) : (b))
#define swap(x,y) {int tmp=x; x=y; y=tmp}

#define _4DEBUG
#define DEBUG_LEVEL 1

#define QUEUE_SIZE 100
#define TRAVEL_TIME 2

#define RAND_KOSMODRON_SPACE 10
#define RAND_ENERGY 1000
#define RAND_SLEEP_TIME 3

#define RESOURCES_NO 3
#define ENERGY 0
#define TUNNEL 1
#define DOCKPLACE 2
#define REPLAY 5
#define RELEASE 6

struct queue_el {
    int event_type;
    int clock;
    int source;
    int value;
};

struct resource_request {
    int clock;
    int ack_left;
};

int size, rank, i;
char processor[100];
MPI_Status status;
int msg[2];

int clock_ = 1;
int planets, systems;
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

#define debug1(...) {if(DEBUG_LEVEL==1 || DEBUG_LEVEL==3){printf(__VA_ARGS__);}}
#define debug2(...) {if(DEBUG_LEVEL==2 || DEBUG_LEVEL==3){printf(__VA_ARGS__);}}

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
        fprintf(fp, "\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n ==== PROCESS nr %d (energy %d) ====\n\n", rank, total_energy);
        if (fp == NULL) {
            printf("%d nie udało się podłączyć do zdalnej konsoli\n", rank);
            MPI_Finalize();
            exit(-1);
        }
    }

    #define printf(...) {fprintf(fp, "%d: ", clock_); fprintf(fp, __VA_ARGS__);}
#else
    #define printf(...) {printf("%d/%d @ %d: ", get_system_no(rank), get_planet_no(rank), clock_); printf(__VA_ARGS__);}
#endif

void my_send(tag, dest) {
    msg[0] = clock_;
    if (dest != rank) {
        debug2("%s %d to %d\n", (tag != REPLAY) ? "REQUEST" : "REPLAY", tag, dest);
        MPI_Send(msg, 2, MPI_INT, dest, tag, MPI_COMM_WORLD);
    }
}

/* helper function */
void queue_add(int event_type, int clock_, int source, int value) {
    for (i = 0; i < QUEUE_SIZE; ++i) {
        if (!queue[i].clock) {
            queue[i].clock = clock_;
            queue[i].event_type = event_type;
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
        MPI_Irecv(msg, 2, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &request);
        MPI_Test(&request, &flag, &status);
        if (flag) { 
             if (status.MPI_TAG == RELEASE) {
                debug2("RELEASE %d from %d @%d\n", msg[1], status.MPI_SOURCE, msg[0]);
                total_energy += msg[1];
            } else if (status.MPI_TAG == REPLAY) {
                debug2("REPLAY %d from %d @%d\n", msg[1], status.MPI_SOURCE, msg[0]);   
                requests[msg[1]].ack_left--;
            } else {
                debug2("REQUEST %d from %d @%d\n", status.MPI_TAG, status.MPI_SOURCE, msg[0]);   
                queue_add(status.MPI_TAG, msg[0], status.MPI_SOURCE, msg[1]);
            }
            clock_ = max(clock_, msg[0]);
        } else {
            MPI_Cancel(&request);
            MPI_Request_free(&request);
            break;
        }
    }

    int i;
    for (i = 0; i < QUEUE_SIZE; i++) {
        if (queue[i].clock) {
            // if (!rank) {
            //     printf("queue[%d] %d %d %d \n", i, queue[i].event_type, requests[queue[i].event_type].clock, queue[i].clock);
            // }
            if (
                requests[queue[i].event_type].clock == 0 || 
                requests[queue[i].event_type].clock > queue[i].clock ||
                (requests[queue[i].event_type].clock == queue[i].clock && queue[i].source < rank)
            ) {
                msg[1] = queue[i].event_type;
                my_send(REPLAY, queue[i].source);
                if (queue[i].event_type == ENERGY) {
                    total_energy -= queue[i].value;
                }
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
        printf("-- NEW SHIP to %d, energy = %d\n", destination, energy);

        // rezerwuj kosmodron
        // debug1("requesting DOCKPLACE...\n");
        // my_wait();
        // clock_++;
        // debug1("requesting DOCKPLACE... DONE\n");

        debug1("requesting ENERGY...\n");
        msg[1] = energy;
        requests[ENERGY].clock = clock_;
        requests[ENERGY].ack_left = planets * 2 - 1;
        for (i = 0; i < systems * planets; ++i) {
            my_send(ENERGY, i);
        }

        my_wait();
        clock_++;
        while (total_energy < energy) {
            work();
        }
        // release energy queue
        requests[ENERGY].clock = 0;
        work();
        debug1("requesting ENERGY... DONE\n");

        debug1("requesting TUNNEL...\n");
        msg[1] = destination;
        requests[TUNNEL].clock = clock_;
        requests[TUNNEL].ack_left = planets * 2 - 1;
        // debug1("requests[%d] left %d\n", TUNNEL, requests[TUNNEL].ack_left);
        for (i = 0; i < planets; ++i) {
            my_send(TUNNEL, get_system_no(destination) * planets + i);
            my_send(TUNNEL, get_system_no(rank) * planets + i);
        }
        my_wait(); 
        clock_++;
        debug1("requesting TUNNEL... DONE\n");


        debug1("-- TRAVELING...\n");
        my_idle(TRAVEL_TIME);
        #ifdef _4DEBUG
            my_idle(3);
        #endif
        debug1("-- TRAVELING... DONE\n");

        debug1("releasing TUNNEL...\n")
        requests[TUNNEL].clock = 0;
        work();
        debug1("releasing TUNNEL... DONE\n")

        debug1("releasing ENERGY...\n")
        msg[1] = energy;
        for (i = 0; i < systems * planets; ++i) {
            my_send(RELEASE, i);
        }
        debug1("releasing ENERGY... DONE\n")
        
        // debug1("releasing DOCKPLACE...\n");
        // debug1("releasing DOCKPLACE... DONE\n");
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

    if (systems * planets > rank) {
        srand(time(NULL) + rank);
        run();
    }

    MPI_Finalize();
    return 0;
}
