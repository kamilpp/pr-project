#include <mpi.h>
#include <fcntl.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <time.h>
#define max(a, b) (((a) > (b)) ? (a) : (b))
#define DEBUG 1

#define swap(x,y) {int tmp=x; x=y; y=tmp}
#define debug(...)  if (DEBUG) printf(__VA_ARGS__)

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

struct queue_el {
    int event_type;
    int clock;
    int source;
    int aa;
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

void my_send(tag, dest) {
    printf("send %d request to %d with clock %d\n", tag, dest, msg[0]);
    MPI_Send(msg, 2, MPI_INT, dest, tag, MPI_COMM_WORLD);
}

/* helper function */
int get_planet_no(int rank) {
    return rank % planets;
}

/* helper function */
int get_system_no(int rank) {
    return rank / systems;   
}

/* helper function */
void queue_add(int event_type, int clock_, int source) {
    for (i = 0; i < QUEUE_SIZE; ++i) {
        if (!queue[i].clock) {
            queue[i].clock = clock_;
            queue[i].event_type = event_type;
            queue[i].source = source;
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
            if (status.MPI_TAG == REPLAY) {
                printf("recieved replay message of %d request from %d with clock %d\n", msg[1], status.MPI_SOURCE, msg[0]);   
                requests[msg[1]].ack_left--;
            } else {
                printf("recieved %d request from %d with clock %d\n", status.MPI_TAG, status.MPI_SOURCE, msg[0]);   
                queue_add(status.MPI_TAG, msg[0], status.MPI_SOURCE);
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
            if (requests[queue[i].event_type].clock == 0 || requests[queue[i].event_type].clock > queue[i].clock) {
                msg[0] = clock_;
                msg[1] = queue[i].event_type;
                printf("sending...\n");
                my_send(REPLAY, queue[i].source);
                // MPI_Send(&msg, 2, MPI_INT, queue[i].source, REPLAY, MPI_COMM_WORLD);
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
        if (i == TUNNEL) printf("AAA requests[%d] left %d\n", TUNNEL, requests[TUNNEL].ack_left);
        while (requests[i].ack_left) {
            work();
        }
        if (i == TUNNEL) printf("BBB requests[%d] left %d\n", TUNNEL, requests[TUNNEL].ack_left);
    }
}

#define _4DEBUG
#ifdef _4DEBUG
    char path[100] = "/dev/pts/";
    FILE *fp;

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
            if (rank == 3) printf("buff: %d %d %s\n", pts, rank, buff);
            if (pts == rank) {
                if (rank == 3) printf("=========================================\n");
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

        printf("%d: %s\n", rank, path);
        fp = fopen(path, "w");
        fprintf(fp, "TEEEST\n");
        if (fp == NULL) {
            printf("%d Blad\n", rank);
        }
    }
#else
    #define printf(...) {printf("%d/%d @ %d: ", get_system_no(rank), get_planet_no(rank), clock_); printf(__VA_ARGS__);}
#endif

void run()
{
    #ifdef _4DEBUG
        init();
    #endif

    // Initialize
    airfield_space = rand() % RAND_KOSMODRON_SPACE;
    airfield_occupied = 0;
    // MPI_Recv(&total_energy, 1, MPI_INT, 0, ENERGY, MPI_COMM_WORLD, &status);

    int energy, destination, sleep_time;
    while (1) {
        if (rank) {
            while(1) {
                work();
            }
        }

        /* rand outgoing ship */
        energy = rand() % RAND_ENERGY + RAND_ENERGY / 5;
        sleep_time = rand() % RAND_SLEEP_TIME + 1;
        do {
            destination = rand() % systems;
            // work();
        }
        while (destination == get_system_no(rank));
        
        /* my_wait before starting */
        // my_idle(sleep_time);
        clock_++;

        printf("%d %d %d %d %d %d\n", total_energy, airfield_space, airfield_occupied, energy, destination, sleep_time);
        debug("new ship to %d, energy = %d\n", destination, energy);
        // rezerwuj kosmodron

        // my_wait();
        clock_++;

        // rezerwuj energię

        // my_wait();
        clock_++;

        // request tunnel
        msg[0] = clock_;
        msg[1] = destination;
        requests[TUNNEL].clock = clock_;
        requests[TUNNEL].ack_left = planets * 2 - 1;
        printf("requests[%d] left %d\n", TUNNEL, requests[TUNNEL].ack_left);
        for (i = 0; i < planets; ++i) {
            my_send(TUNNEL, destination * planets + i);
            // MPI_Send(&msg, 2, MPI_INT, get_system_no(destination) + i, TUNNEL, MPI_COMM_WORLD);
            if (get_system_no(rank) + i != rank) {
                my_send(TUNNEL, get_system_no(rank) * planets + i);
                // MPI_Send(&msg, 2, MPI_INT, get_system_no(rank) + i, TUNNEL, MPI_COMM_WORLD);
            }
        }
        my_wait();
        printf("222222requests[%d] left %d\n", TUNNEL, requests[TUNNEL].ack_left);
        clock_++;

        // travel
        my_idle(TRAVEL_TIME);

        // release tunnel
        requests[TUNNEL].clock = 0;
        // release energy
        requests[ENERGY].clock = 0;
        // release dock place (send )

        // fire release!
        work();
    }
}

int main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Get_processor_name(processor, &i);

    if (argc < 3) {
        // MPI_Finalize();
        if (!rank) {
            printf("Usage: ./nazwa [liczba ukladow] [liczba planet]\n");
        }
        exit(-1);
    }

    systems = atoi(argv[1]);
    planets = atoi(argv[2]);
    total_energy = RAND_ENERGY * planets * systems * RAND_KOSMODRON_SPACE / 4;

    if (systems * planets > rank) {
        // if (!rank) {
        //     // initilize energy
            // total_energy = RAND_ENERGY * planets * systems * RAND_KOSMODRON_SPACE / 4;
        //     for (i = 0; i < systems * planets; ++i) {
        //         MPI_Send(&total_energy, 1, MPI_INT, i, ENERGY, MPI_COMM_WORLD);
        //     }
        // }

        run();
    }

    MPI_Finalize();
    return 0;
}
