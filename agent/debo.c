#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>
#include <errno.h>
#include <pthread.h>
#include <sys/epoll.h>
#include <signal.h>
#include <stdbool.h>
#include <fcntl.h>

#include "connutil.h"
#include "format.h"
#include "comm.h"
#include "gsignal.h"
#include "stringinfo.h"
#include "action.h"
#include "configuration.h"
#include "install.h"
#include "report.h"
#include "uninstall.h"
#include "protocol.h"
#include "latch.h"

#define DBINVALID_SOCKET (-1)
#define WAIT_USE_EPOLL

struct Latch *MyLatch;
int                     MyProcPid;

#define lengthof(array) (sizeof (array) / sizeof ((array)[0]))


#define STATUS_OK                               (0)
#define STATUS_ERROR                    (-1)
#define STATUS_EOF                              (-2)


#define MemSet(start, val, len) \
    do \
{ \
    /* must be void* because we don't know if it is integer aligned yet */ \
    void   *_vstart = (void *) (start); \
    int             _val = (val); \
    Size    _len = (len); \
    \
    if ((((uintptr_t) _vstart) & LONG_ALIGN_MASK) == 0 && \
        (_len & LONG_ALIGN_MASK) == 0 && \
        _val == 0 && \
        _len <= MEMSET_LOOP_LIMIT && \
        /* \
         *      If MEMSET_LOOP_LIMIT == 0, optimizer should find \
         *      the whole "if" false at compile time. \
         */ \
         MEMSET_LOOP_LIMIT != 0) \
    { \
        long *_start = (long *) _vstart; \
        long *_stop = (long *) ((char *) _start + _len); \
        while (_start < _stop) \
        *_start++ = 0; \
    } \
    else \
    memset(_vstart, _val, _len); \
} while (0)

int                     MyPMChildSlot;

/* event multiplexing object */
static WaitEventSet *pm_wait_set;

/* Socket configuration */
int PostPortNumber = 1221;
char *ListenAddresses = "0.0.0.0,::";  /* IPv4 + IPv6 wildcards */

/* Socket management */
#define MAXLISTEN 64
static int NumListenSockets = 0;
static int ListenSockets[MAXLISTEN];


ClientSocket *global_client_socket = NULL;

volatile sig_atomic_t shutdown_requested = 0;
/* Event system structures */

void handle_signal() {
    shutdown_requested = 1;
}

typedef enum { CLIENT_ACTIVE, CLIENT_CLOSED } HandleCommandResult;


/*
 * Mark a point as unreachable in a portable fashion.  This should preferably
 * be something that the compiler understands, to aid code generation.
 * In assert-enabled builds, we prefer abort() for debugging reasons.
 */
#if defined(HAVE__BUILTIN_UNREACHABLE) && !defined(USE_ASSERT_CHECKING)
#define db_unreachable() __builtin_unreachable()
#elif defined(_MSC_VER) && !defined(USE_ASSERT_CHECKING)
#define db_unreachable() __assume(0)
#else
#define db_unreachable() abort()
#endif


typedef struct WaitEventSet {
    int nevents;
    int nevents_space;
    WaitEvent *events;
#if defined(WAIT_USE_EPOLL)
    int epoll_fd;
    struct epoll_event *epoll_ret_events;
#elif defined(WAIT_USE_KQUEUE)
    int kqueue_fd;
    struct kevent *kqueue_ret_events;
#elif defined(WAIT_USE_POLL)
    struct pollfd *pollfds;
#elif defined(WAIT_USE_WIN32)
    HANDLE *handles;
#endif
} WaitEventSet;

/* Platform-specific initialization */
#ifdef _WIN32
static WSADATA wsaData;
#define closesocket(s) closesocket(s)
#else
#define closesocket(s) close(s)
#endif

static void handle_command(ClientSocket *client);
static int AgentLoop(void);
static int
BackendStartup(ClientSocket *client_sock);

/* Event system implementation */
WaitEventSet *CreateConnectionEventSet(int nevents) {
    WaitEventSet *set = malloc(sizeof(WaitEventSet));
    if (!set) return NULL;

    memset(set, 0, sizeof(*set));
    set->nevents = 0;
    set->nevents_space = nevents;
    set->events = malloc(sizeof(WaitEvent) * nevents);

#if defined(WAIT_USE_EPOLL)
    set->epoll_fd = epoll_create1(EPOLL_CLOEXEC);
    set->epoll_ret_events = malloc(sizeof(struct epoll_event) * nevents);
#elif defined(WAIT_USE_KQUEUE)
    set->kqueue_fd = kqueue();
    set->kqueue_ret_events = malloc(sizeof(struct kevent) * nevents);
#elif defined(WAIT_USE_POLL)
    set->pollfds = malloc(sizeof(struct pollfd) * nevents);
#elif defined(WAIT_USE_WIN32)
    set->handles = malloc(sizeof(HANDLE) * (nevents + 1));
#endif

    return set;
}




/* Main server initialization */
int main() {

    signal(SIGINT, handle_signal);
    signal(SIGTERM, handle_signal);
#ifdef _WIN32
    if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0) {
        fprintf(stderr, "WSAStartup failed\n");
        exit(EXIT_FAILURE);
    }
#endif

    char *hostlist = strdup(ListenAddresses);
    char *curhost = strtok(hostlist, ",");

    while (curhost != NULL && NumListenSockets < MAXLISTEN) {
        int status = ListenServerPort(AF_UNSPEC, curhost, PostPortNumber, ListenSockets, &NumListenSockets,
                                      MAXLISTEN);

        if (status != 0) {
            fprintf(stderr, "Failed to create socket for '%d'\n", ListenSockets[NumListenSockets]);
        }
        curhost = strtok(NULL, ",");
    }
    free(hostlist);

    if (NumListenSockets == 0) {
        fprintf(stderr, "No valid listen sockets created\n");
        exit(EXIT_FAILURE);
    }

    int  status = AgentLoop();

    /*
     * ServerLoop probably shouldn't ever return, but if it does, close down.
     */
    if(status != STATUS_OK)
        exit(EXIT_FAILURE);
}



int AddSocketEvent(WaitEventSet *set, uint32_t events, int fd, void *user_data) {
    if (set->nevents >= set->nevents_space || fd < 0)
        return -1;

    WaitEvent *event = &set->events[set->nevents];
    event->pos = set->nevents++;
    event->fd = fd;
    event->events = events;
    event->user_data = user_data;

#if defined(WAIT_USE_EPOLL)
    struct epoll_event epevent;
    epevent.events = EPOLLIN | (events == WL_SOCKET_ACCEPT ? EPOLLET : 0);
    epevent.data.ptr = event;
    epoll_ctl(set->epoll_fd, EPOLL_CTL_ADD, fd, &epevent);
#elif defined(WAIT_USE_KQUEUE)
    struct kevent kev;
    EV_SET(&kev, fd, EVFILT_READ, EV_ADD | EV_ENABLE, 0, 0, event);
    kevent(set->kqueue_fd, &kev, 1, NULL, 0, NULL);
#elif defined(WAIT_USE_POLL)
    set->pollfds[event->pos].fd = fd;
    set->pollfds[event->pos].events = POLLIN;
#elif defined(WAIT_USE_WIN32)
    WSAEventSelect(fd, set->handles[event->pos + 1], FD_READ | FD_ACCEPT);
#endif

    return event->pos;
}

/*
 * Main idle loop of postmaster
 */
static int
AgentLoop(void)
{

    WaitEventSet *event_set = CreateConnectionEventSet(MAXLISTEN);

    /* Register all listening sockets */
    for (int i = 0; i < NumListenSockets; i++) {
        AddSocketEvent(event_set, WL_SOCKET_ACCEPT, ListenSockets[i], NULL);
    }

    while (!shutdown_requested){
        int nevents = 0; // Initialize to 0
        WaitEvent events[MAXLISTEN];

#if defined(WAIT_USE_EPOLL)
        nevents = epoll_wait(event_set->epoll_fd, event_set->epoll_ret_events,
                             MAXLISTEN, -1);
        /* Convert epoll events to generic format */
        for (int i = 0; i < nevents; i++) {
            events[i] = *(WaitEvent*)event_set->epoll_ret_events[i].data.ptr;
        }
#elif defined(WAIT_USE_KQUEUE)
        struct timespec timeout = {0};
        nevents = kevent(event_set->kqueue_fd, NULL, 0,
                         event_set->kqueue_ret_events, MAXLISTEN, &timeout);
        /* Convert kqueue events */
        for (int i = 0; i < nevents; i++) {
            events[i] = *(WaitEvent*)event_set->kqueue_ret_events[i].udata;
        }
#elif defined(WAIT_USE_POLL)
        nevents = poll(event_set->pollfds, event_set->nevents, -1);
        /* Convert poll events */
        int event_count = 0;
        for (int i = 0; i < event_set->nevents; i++) {
            if (event_set->pollfds[i].revents & POLLIN) {
                events[event_count++] = event_set->events[i];
            }
        }
        nevents = event_count; // Correct the nevents after processing
#elif defined(WAIT_USE_WIN32)
        nevents = WSAWaitForMultipleEvents(event_set->nevents,
                                           event_set->handles, FALSE,
                                           WSA_INFINITE, FALSE);
        /* Handle Windows events */
        // ... Windows-specific event processing ...
#endif

        /* Process all events */

        for (int i = 0; i < nevents; i++)
        {
            if (events[i].events & WL_SOCKET_ACCEPT)
            {
                ClientSocket s;

                if (AcceptConnection(events[i].fd, &s) == STATUS_OK)
                    BackendStartup(&s);

                /* Close client socket in the postmaster process */
                if (s.sock != DBINVALID_SOCKET)
                {
                    if (closesocket(s.sock) != 0)
                        fprintf(stderr, "could not close client socket");
                }
            }
        }

    }
    return 0;
}

void
CloseDeboPorts(void)
{
    /* Release resources held by  WaitEventSet. */
    if (pm_wait_set)
    {
        FreeWaitEventSetAfterFork(pm_wait_set);
        pm_wait_set = NULL;
    }
    for (int i = 0; i < NumListenSockets; i++) {
        if (ListenSockets[i] != DBINVALID_SOCKET) {
            closesocket(ListenSockets[i]);  // Close the socket FD
            ListenSockets[i] = DBINVALID_SOCKET;  // Mark as closed
        }
    }
    NumListenSockets = 0;
    //ListenSockets = NULL;

}

pid_t
fork_process(void)
{
    pid_t		result;
    //sigset_t	save_mask;

#ifdef LINUX_PROFILE
    struct itimerval prof_itimer;
#endif

    /*
     * Flush stdio channels just before fork, to avoid double-output problems.
     */
    fflush(NULL);

#ifdef LINUX_PROFILE

    /*
     * Linux's fork() resets the profiling timer in the child process. If we
     * want to profile child processes then we need to save and restore the
     * timer setting.  This is a waste of time if not profiling, however, so
     * only do it if commanded by specific -DLINUX_PROFILE switch.
     */
    getitimer(ITIMER_PROF, &prof_itimer);
#endif

    //sigprocmask(SIG_SETMASK, &BlockSig, &save_mask);
    result = fork();
    if (result == 0)
    {
        //printf("Child (PID: %d) continuing execution.\n", getpid());
        //sleep(50);
        /* fork succeeded, in child */
        //MyProcPid = getpid();
#ifdef LINUX_PROFILE
        setitimer(ITIMER_PROF, &prof_itimer, NULL);
#endif

    }

    return result;
}

static void
CopySockAddr(SockAddr *dest, const SockAddr *src)
{
    // Copy the address storage and length
    memcpy(&dest->addr, &src->addr, sizeof(struct sockaddr_storage));
    dest->salen = src->salen;
}

pid_t
debo_child_launch(ClientSocket *client_sock)
{
    pid_t pid = fork_process();
    if (pid == 0) {  // Child process
                     // Close parent's listening sockets (no memory deallocation!)
        CloseDeboPorts();
        // Deep copy the entire ClientSocket
        ClientSocket *MyClientSocket = malloc(sizeof(ClientSocket));
        if (!MyClientSocket) {
            fprintf(stderr, "Child: malloc failed\n");
            _exit(1);
        }
        int flags = fcntl(client_sock->sock, F_GETFL, 0);
        fcntl(client_sock->sock, F_SETFL, flags & ~O_NONBLOCK);  // Disable non-blocking mode

        // Copy the socket FD and SockAddr
        MyClientSocket->sock = client_sock->sock;  // Integer copy is safe
        CopySockAddr(&MyClientSocket->raddr, &client_sock->raddr);  // Deep copy
                                                                    // Perform GSSAPI handshake
        if (secure_open_gssapi(MyClientSocket) != 0)
        {
            fprintf(stderr, "GSSAPI handshake failed\n");
        }
        // Handle the client command
        handle_command(MyClientSocket);

        // Cleanup: Close socket and free memory
        closesocket(MyClientSocket->sock);
        free(MyClientSocket);

        // Use _exit() to avoid flushing parent's I/O buffers
        _exit(0);
    }
    return pid;
}


/*
 * BackendStartup -- start backend process
 *
 * returns: STATUS_ERROR if the fork failed, STATUS_OK otherwise.
 */
static int
BackendStartup(ClientSocket *client_sock)
{
    pid_t		pid;



    pid = debo_child_launch(client_sock);
    if (pid < 0)
    {
        /* in parent, fork failed */
        int			save_errno = errno;

        errno = save_errno;
        fprintf(stderr, "could not fork new process for connection:\n");
        return STATUS_ERROR;
    }

    return STATUS_OK;
}



#define MAX_LIMIT 1024

static void handle_command(ClientSocket *client_socket) {
    StringInfoData param_buffer;
    StringInfoData value_buffer;
    initStringInfo(&param_buffer);
    initStringInfo(&value_buffer);
    global_client_socket = client_socket;
    for (;;) {
        resetStringInfo(&param_buffer);
        int action_code = getbyte(client_socket);
        if (getmessage(&param_buffer, client_socket, MAX_LIMIT))
            free(param_buffer.data);
        if (action_code == EOF || action_code == CliMsg_Finish) {
            close(client_socket->sock);
            return;  // Let ServerLoop free client_socket and update event_set
        }

        char **result = split_string(param_buffer.data);
        printf(" the  data %s", param_buffer.data);
        // printf(" first second data %s", result[0]);
        //printf(" first second data %s", result[1]);

        switch (action_code) {
            /* ===================== HDFS Commands ===================== */
        case CliMsg_Hdfs_Start:
            if (!isComponentInstalled(HDFS)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(HDFS));
                break;
            }
            hadoop_action(START);
            break;
        case CliMsg_Hdfs_Stop:
            if (!isComponentInstalled(HDFS)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(HDFS));
                break;
            }
            hadoop_action(STOP);
            break;
        case CliMsg_Hdfs_Restart:
            if (!isComponentInstalled(HDFS)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(HDFS));
                break;
            }
            hadoop_action(RESTART);
            break;
        case CliMsg_Hdfs_Uninstall:
            if (!isComponentInstalled(HDFS)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(HDFS));
                break;
            }
            uninstall_hadoop();
            break;
        case CliMsg_Hdfs_Install_Version:
            if (isComponentInstalled(HDFS)){
                PRINTF(global_client_socket, "%s is already installed.\n", component_to_string(HDFS));
                break;
            }
            install_hadoop(result[0],result[1]);
            configure_target_component(HDFS);
            break;
        case CliMsg_Hdfs_Install:
            if (isComponentInstalled(HDFS)){
                PRINTF(global_client_socket, "%s is already installed.\n", component_to_string(HDFS));
                break;
            }
            install_hadoop(NULL, NULL);
            configure_target_component(HDFS);
            break;
        case CliMsg_Hdfs:
            if (!isComponentInstalled(HDFS)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(HDFS));
                break;
            }
            FPRINTF(global_client_socket, report_hdfs());
            break;


            /* ===================== Spark Commands ==================== */
        case CliMsg_Spark_Start:
            if (!isComponentInstalled(SPARK)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(SPARK));
                break;
            }
            spark_action(START);
            break;
        case CliMsg_Spark_Stop:
            if (!isComponentInstalled(SPARK)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(SPARK));
                break;
            }
            spark_action(STOP);
            break;
        case CliMsg_Spark_Restart:
            if (!isComponentInstalled(SPARK)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(SPARK));
                break;
            }
            spark_action(RESTART);
            break;
        case CliMsg_Spark_Uninstall:
            if (!isComponentInstalled(SPARK)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(SPARK));
                break;
            }
            uninstall_spark();
            break;
        case CliMsg_Spark_Install_Version:
            if (isComponentInstalled(SPARK)){
                PRINTF(global_client_socket, "%s is already installed.\n", component_to_string(SPARK));
                break;
            }
            install_spark(result[0],result[1]);
            configure_target_component(SPARK);
            break;
        case CliMsg_Spark_Install:
            if (isComponentInstalled(SPARK)){
                PRINTF(global_client_socket, "%s is already installed.\n", component_to_string(SPARK));
                break;
            }
            install_spark(NULL, NULL);
            configure_target_component(SPARK);
            break;
        case CliMsg_Spark:
            if (!isComponentInstalled(SPARK)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(SPARK));
                break;
            }
            FPRINTF(global_client_socket, report_spark());
            break;
            /* ===================== Kafka Commands ===================== */
        case CliMsg_Kafka_Start:
            if (!isComponentInstalled(KAFKA)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(KAFKA));
                break;
            }
            kafka_action(START);
            break;
        case CliMsg_Kafka_Stop:
            if (!isComponentInstalled(KAFKA)){
                PRINTF(global_client_socket, "%s is not installed.\n", component_to_string(KAFKA));
                break;
            }
            kafka_action(STOP);
            break;
        case CliMsg_Kafka_Restart:
            if (!isComponentInstalled(KAFKA)){
                PRINTF(global_client_socket, "%s is not installed.\n", component_to_string(KAFKA));
                break;
            }
            kafka_action(RESTART);
            break;
        case CliMsg_Kafka_Uninstall:
            if (!isComponentInstalled(KAFKA)){
                PRINTF(global_client_socket, "%s is not installed.\n", component_to_string(KAFKA));
                break;
            }
            uninstall_kafka();
            break;
        case CliMsg_Kafka_Install_Version:
            if (isComponentInstalled(KAFKA)){
                PRINTF(global_client_socket, "%s is already installed.\n", component_to_string(KAFKA));
                break;
            }
            install_kafka(result[0],result[1]);
            configure_target_component(KAFKA);
            break;
        case CliMsg_Kafka_Install:
            if (isComponentInstalled(KAFKA)){
                PRINTF(global_client_socket, "%s is already installed.\n", component_to_string(KAFKA));
                break;
            }
            install_kafka(NULL, NULL);
            configure_target_component(KAFKA);
            break;
        case CliMsg_Kafka:
            if (!isComponentInstalled(KAFKA)){
                PRINTF(global_client_socket, "%s is not installed.\n", component_to_string(KAFKA));
                break;
            }
            FPRINTF(global_client_socket, report_kafka());
            break;

            /* ===================== HBase Commands ===================== */
        case CliMsg_HBase_Start:
            if (!isComponentInstalled(HBASE)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(HBASE));
                break;
            }
            HBase_action(START);
            break;
        case CliMsg_HBase_Stop:
            if (!isComponentInstalled(HBASE)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(HBASE));
                break;
            }
            HBase_action(STOP);
            break;
        case CliMsg_HBase_Restart:
            if (!isComponentInstalled(HBASE)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(HBASE));
                break;
            }
            HBase_action(RESTART);
            break;
        case CliMsg_HBase_Uninstall:
            if (!isComponentInstalled(HBASE)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(HBASE));
                break;
            }
            uninstall_HBase();
            break;
        case CliMsg_HBase_Install_Version:
            if (isComponentInstalled(HBASE)){
                PRINTF(global_client_socket, "%s is already installed.\n", component_to_string(HBASE));
                break;
            }
            install_HBase(result[0],result[1]);
            configure_target_component(HBASE);
            break;
        case CliMsg_HBase_Install:
            if (isComponentInstalled(HBASE)){
                PRINTF(global_client_socket, "%s is already installed.\n", component_to_string(HBASE));
                break;
            }
            install_HBase(NULL, NULL);
            configure_target_component(HBASE);
            break;
        case CliMsg_HBase:
            if (!isComponentInstalled(HBASE)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(HBASE));
                break;
            }
            FPRINTF(global_client_socket, report_hbase());
            break;

            /* =================== ZooKeeper Commands =================== */
        case CliMsg_ZooKeeper_Start:
            if (!isComponentInstalled(ZOOKEEPER)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(ZOOKEEPER));
                break;
            }
            zookeeper_action(START);
            break;
        case CliMsg_ZooKeeper_Stop:
            if (!isComponentInstalled(ZOOKEEPER)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(ZOOKEEPER));
                break;
            }
            zookeeper_action(STOP);
            break;
        case CliMsg_ZooKeeper_Restart:
            if (!isComponentInstalled(ZOOKEEPER)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(ZOOKEEPER));
                break;
            }
            zookeeper_action(RESTART);
            break;
        case CliMsg_ZooKeeper_Uninstall:
            if (!isComponentInstalled(ZOOKEEPER)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(ZOOKEEPER));
                break;
            }
            uninstall_zookeeper();
            break;
        case CliMsg_ZooKeeper_Install_Version:
            if (isComponentInstalled(ZOOKEEPER)){
                PRINTF(global_client_socket, "%s is already installed.\n", component_to_string(ZOOKEEPER));
                break;
            }
            install_zookeeper(result[0],result[1]);
            configure_target_component(ZOOKEEPER);
            break;
        case CliMsg_ZooKeeper_Install:
            if (isComponentInstalled(ZOOKEEPER)){
                PRINTF(global_client_socket, "%s is already installed.\n", component_to_string(ZOOKEEPER));
                break;
            }
            install_zookeeper(NULL, NULL);
            configure_target_component(ZOOKEEPER);
            break;
        case CliMsg_ZooKeeper:
            if (!isComponentInstalled(ZOOKEEPER)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(ZOOKEEPER));
                break;
            }
            FPRINTF(global_client_socket, report_zookeeper());
            break;

            /* ===================== Flink Commands ===================== */
        case CliMsg_Flink_Start:
            if (!isComponentInstalled(FLINK)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(FLINK));
                break;
            }
            flink_action(START);
            break;
        case CliMsg_Flink_Stop:
            if (!isComponentInstalled(FLINK)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(FLINK));
                break;
            }
            flink_action(STOP);
            break;
        case CliMsg_Flink_Restart:
            if (!isComponentInstalled(FLINK)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(FLINK));
                break;
            }
            flink_action(RESTART);
            break;
        case CliMsg_Flink_Uninstall:
            if (!isComponentInstalled(FLINK)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(FLINK));
                break;
            }
            uninstall_flink();
            break;
        case CliMsg_Flink_Install_Version:
            if (isComponentInstalled(FLINK)){
                PRINTF(global_client_socket, "%s is already installed.\n", component_to_string(FLINK));
                break;
            }
            install_flink(result[0],result[1]);
            configure_target_component(FLINK);
            break;
        case CliMsg_Flink_Install:
            if (isComponentInstalled(FLINK)){
                PRINTF(global_client_socket, "%s is already installed.\n", component_to_string(FLINK));
                break;
            }
            install_flink(NULL, NULL);
            configure_target_component(FLINK);
            break;
        case CliMsg_Flink:
            if (!isComponentInstalled(FLINK)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(FLINK));
                break;
            }
            FPRINTF(global_client_socket, report_flink());
            break;

            /* ===================== Storm Commands ===================== */
        case CliMsg_Storm_Start:
            if (!isComponentInstalled(STORM)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(STORM));
                break;
            }
            storm_action(START);
            break;
        case CliMsg_Storm_Stop:
            if (!isComponentInstalled(STORM)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(STORM));
                break;
            }
            storm_action(STOP);
            break;
        case CliMsg_Storm_Restart:
            if (!isComponentInstalled(STORM)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(STORM));
                break;
            }
            storm_action(RESTART);
            break;
        case CliMsg_Storm_Uninstall:
            if (!isComponentInstalled(STORM)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(STORM));
                break;
            }
            uninstall_Storm();
            break;
        case CliMsg_Storm_Install_Version:
            if (isComponentInstalled(STORM)){
                PRINTF(global_client_socket, "%s is already installed.\n", component_to_string(STORM));
                break;
            }
            install_Storm(result[0],result[1]);
            configure_target_component(STORM);
            break;
        case CliMsg_Storm_Install:
            if (isComponentInstalled(STORM)){
                PRINTF(global_client_socket, "%s is already installed.\n", component_to_string(STORM));
                break;
            }
            install_Storm(NULL, NULL);
            configure_target_component(STORM);
            break;
        case CliMsg_Storm:
            if (!isComponentInstalled(STORM)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(STORM));
                break;
            }
            PRINTF(global_client_socket, report_storm());
            break;

            /* ===================== Hive Commands ====================== */
        case CliMsg_Hive_Start:
            if (!isComponentInstalled(HIVE)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(HIVE));
                break;
            }
            hive_action(START);
            break;
        case CliMsg_Hive_Stop:
            if (!isComponentInstalled(HIVE)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(HIVE));
                break;
            }
            hive_action(STOP);
            break;
        case CliMsg_Hive_Restart:
            if (!isComponentInstalled(HIVE)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(HIVE));
                break;
            }
            hive_action(RESTART);
            break;
        case CliMsg_Hive_Uninstall:
            if (!isComponentInstalled(HIVE)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(HIVE));
                break;
            }
            uninstall_hive();
            break;
        case CliMsg_Hive_Install_Version:
            if (isComponentInstalled(HIVE)){
                PRINTF(global_client_socket, "%s is already installed.\n", component_to_string(HIVE));
                break;
            }
            install_hive(result[0],result[1]);
            configure_target_component(HIVE);
            break;
        case CliMsg_Hive_Install:
            if (isComponentInstalled(HIVE)){
                PRINTF(global_client_socket, "%s is already installed.\n", component_to_string(HIVE));
                break;
            }
            install_hive(NULL, NULL);
            configure_target_component(HIVE);
            break;
        case CliMsg_Hive:
            if (!isComponentInstalled(HIVE)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(HIVE));
                break;
            }
            FPRINTF(global_client_socket, report_hive());
            break;

            /* ===================== Pig Commands ====================== */
        case CliMsg_Pig_Start:
            if (!isComponentInstalled(PIG)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(PIG));
                break;
            }
            pig_action(START);
            break;
        case CliMsg_Pig_Stop:
            if (!isComponentInstalled(PIG)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(PIG));
                break;
            }
            pig_action(STOP);
            break;
        case CliMsg_Pig_Restart:
            if (!isComponentInstalled(PIG)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(PIG));
                break;
            }
            pig_action(RESTART);
            break;
        case CliMsg_Pig_Uninstall:
            if (!isComponentInstalled(PIG)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(PIG));
                break;
            }
            uninstall_pig();
            break;
        case CliMsg_Pig_Install_Version:
            if (isComponentInstalled(PIG)){
                PRINTF(global_client_socket, "%s is already installed.\n", component_to_string(PIG));
                break;
            }
            install_pig(result[0],result[1]);
            configure_target_component(PIG);
            break;
        case CliMsg_Pig_Install:
            if (isComponentInstalled(PIG)){
                PRINTF(global_client_socket, "%s is already installed.\n", component_to_string(PIG));
                break;
            }
            install_pig(NULL, NULL);
            configure_target_component(PIG);
            break;
        case CliMsg_Pig:
            if (!isComponentInstalled(PIG)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(PIG));
                break;
            }
            FPRINTF(global_client_socket, report_pig());
            break;

            /* ===================== Tez Commands ====================== */
        case CliMsg_Tez_Start:
            if (!isComponentInstalled(TEZ)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(TEZ));
                break;
            }
            tez_action(START);
            break;
        case CliMsg_Tez_Stop:
            if (!isComponentInstalled(TEZ)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(TEZ));
                break;
            }
            tez_action(STOP);
            break;
        case CliMsg_Tez_Restart:
            if (!isComponentInstalled(TEZ)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(TEZ));
                break;
            }
            tez_action(RESTART);
            break;
        case CliMsg_Tez_Uninstall:
            if (!isComponentInstalled(TEZ)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(TEZ));
                break;
            }
            uninstall_Tez();
            break;
        case CliMsg_Tez_Install_Version:
            if (isComponentInstalled(TEZ)){
                PRINTF(global_client_socket, "%s is already installed.\n", component_to_string(TEZ));
                break;
            }
            install_Tez(result[0],result[1]);
            configure_target_component(TEZ);
            break;
        case CliMsg_Tez_Install:
            if (isComponentInstalled(TEZ)){
                PRINTF(global_client_socket, "%s is already installed.\n", component_to_string(TEZ));
                break;
            }
            install_Tez(NULL, NULL);
            configure_target_component(TEZ);
            break;
        case CliMsg_Tez:
            if (!isComponentInstalled(TEZ)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(TEZ));
                break;
            }
            FPRINTF(global_client_socket, report_tez());
            break;

            /* ==================== Atlas Commands ===================== */
        case CliMsg_Atlas_Start:
            if (!isComponentInstalled(ATLAS)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(ATLAS));
                break;
            }
            atlas_action(START);
            break;
        case CliMsg_Atlas_Stop:
            if (!isComponentInstalled(ATLAS)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(ATLAS));
                break;
            }
            atlas_action(STOP);
            break;
        case CliMsg_Atlas_Restart:
            if (!isComponentInstalled(ATLAS)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(ATLAS));
                break;
            }
            atlas_action(RESTART);
            break;
        case CliMsg_Atlas_Uninstall:
            if (!isComponentInstalled(ATLAS)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(ATLAS));
                break;
            }
            uninstall_Atlas();
            break;
        case CliMsg_Atlas_Install_Version:
            if (isComponentInstalled(ATLAS)){
                PRINTF(global_client_socket, "%s is already installed.\n", component_to_string(ATLAS));
                break;
            }
            install_Atlas(result[0],result[1]);
            configure_target_component(ATLAS);
            break;
        case CliMsg_Atlas_Install:
            if (isComponentInstalled(ATLAS)){
                PRINTF(global_client_socket, "%s is already installed.\n", component_to_string(ATLAS));
                break;
            }
            install_Atlas(NULL, NULL);
            configure_target_component(ATLAS);
            break;
        case CliMsg_Atlas:
            if (!isComponentInstalled(ATLAS)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(ATLAS));
                break;
            }
            FPRINTF(global_client_socket, report_atlas());
            break;

            /* ==================== Ranger Commands ==================== */
        case CliMsg_Ranger_Start:
            if (!isComponentInstalled(RANGER)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(RANGER));
                break;
            }
            ranger_action(START);
            break;
        case CliMsg_Ranger_Stop:
            if (!isComponentInstalled(RANGER)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(RANGER));
                break;
            }
            ranger_action(STOP);
            break;
        case CliMsg_Ranger_Restart:
            if (!isComponentInstalled(RANGER)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(RANGER));
                break;
            }
            ranger_action(RESTART);
            break;
        case CliMsg_Ranger_Uninstall:
            if (!isComponentInstalled(RANGER)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(RANGER));
                break;
            }
            uninstall_ranger();
            break;
        case CliMsg_Ranger_Install_Version:
            if (isComponentInstalled(RANGER)){
                PRINTF(global_client_socket, "%s is already installed.\n", component_to_string(RANGER));
                break;
            }
            install_Ranger(result[0],result[1]);
            configure_target_component(RANGER);
            break;
        case CliMsg_Ranger_Install:
            if (isComponentInstalled(RANGER)){
                PRINTF(global_client_socket, "%s is already installed.\n", component_to_string(RANGER));
                break;
            }
            install_Ranger(NULL, NULL);
            configure_target_component(RANGER);
            break;
        case CliMsg_Ranger:
            if (!isComponentInstalled(RANGER)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(RANGER));
                break;
            }
            FPRINTF(global_client_socket, report_ranger());
            break;

            /* ===================== Livy Commands ===================== */
        case CliMsg_Livy_Start:
            if (!isComponentInstalled(LIVY)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(LIVY));
                break;
            }
            livy_action(START);
            break;
        case CliMsg_Livy_Stop:
            if (!isComponentInstalled(LIVY)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(LIVY));
                break;
            }
            livy_action(STOP);
            break;
        case CliMsg_Livy_Restart:
            if (!isComponentInstalled(LIVY)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(LIVY));
                break;
            }
            livy_action(RESTART);
            break;
        case CliMsg_Livy_Uninstall:
            if (!isComponentInstalled(LIVY)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(LIVY));
                break;
            }
            uninstall_livy();
            break;
        case CliMsg_Livy_Install_Version:
            if (isComponentInstalled(LIVY)){
                PRINTF(global_client_socket, "%s is already installed.\n", component_to_string(LIVY));
                break;
            }
            install_Livy(result[0],result[1]);
            configure_target_component(LIVY);
            break;
        case CliMsg_Livy_Install:
            if (isComponentInstalled(LIVY)){
                PRINTF(global_client_socket, "%s is already installed.\n", component_to_string(LIVY));
                break;
            }
            install_Livy(NULL, NULL);
            configure_target_component(LIVY);
            break;
        case CliMsg_Livy:
            if (!isComponentInstalled(LIVY)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(LIVY));
                break;
            }
            FPRINTF(global_client_socket, report_livy());
            break;

            /* =================== Phoenix Commands =================== */
        case CliMsg_Phoenix_Start:
            if (!isComponentInstalled(PHOENIX)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(PHOENIX));
                break;
            }
            phoenix_action(START);
            break;
        case CliMsg_Phoenix_Stop:
            if (!isComponentInstalled(PHOENIX)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(PHOENIX));
                break;
            }
            phoenix_action(STOP);
            break;
        case CliMsg_Phoenix_Restart:
            if (!isComponentInstalled(PHOENIX)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(PHOENIX));
                break;
            }
            phoenix_action(RESTART);
            break;
        case CliMsg_Phoenix_Uninstall:
            if (!isComponentInstalled(PHOENIX)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(PHOENIX));
                break;
            }
            uninstall_phoenix();
            break;
        case CliMsg_Phoenix_Install_Version:
            if (isComponentInstalled(PHOENIX)){
                PRINTF(global_client_socket, "%s is already installed.\n", component_to_string(PHOENIX));
                break;
            }
            install_phoenix(result[0],result[1]);
            configure_target_component(PHOENIX);
            break;
            /* =================== Phoenix Commands =================== */
        case CliMsg_Phoenix_Install:
            if (isComponentInstalled(PHOENIX)){
                PRINTF(global_client_socket, "%s is already installed.\n", component_to_string(PHOENIX));
                break;
            }
            install_phoenix(NULL, NULL);
            configure_target_component(PHOENIX);
            break;
        case CliMsg_Phoenix:
            if (!isComponentInstalled(PHOENIX)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(PHOENIX));
                break;
            }
            PRINTF(global_client_socket, report_phoenix());
            break;

            /* ===================== Solr Commands =================== */
        case CliMsg_Solr_Start:
            if (!isComponentInstalled(SOLR)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(SOLR));
                break;
            }
            Solr_action(START);
            break;
        case CliMsg_Solr_Stop:
            if (!isComponentInstalled(SOLR)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(SOLR));
                break;
            }
            Solr_action(STOP);
            break;
        case CliMsg_Solr_Restart:
            if (!isComponentInstalled(SOLR)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(SOLR));
                break;
            }
            Solr_action(RESTART);
            break;
        case CliMsg_Solr_Uninstall:
            if (!isComponentInstalled(SOLR)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(SOLR));
                break;
            }
            uninstall_Solr();
            break;
        case CliMsg_Solr_Install_Version:
            if (isComponentInstalled(SOLR)){
                PRINTF(global_client_socket, "%s is already installed.\n", component_to_string(SOLR));
                break;
            }
            install_Solr(result[0],result[1]);
            configure_target_component(SOLR);
            break;
        case CliMsg_Solr_Install:
            if (isComponentInstalled(SOLR)){
                PRINTF(global_client_socket, "%s is already installed.\n", component_to_string(SOLR));
                break;
            }
            install_Solr(NULL, NULL);
            configure_target_component(SOLR);
            break;
        case CliMsg_Solr:
            if (!isComponentInstalled(SOLR)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(SOLR));
                break;
            }
            PRINTF(global_client_socket, report_solr());
            break;

            /* =================== Zeppelin Commands ================== */
        case CliMsg_Zeppelin_Stop:
            if (!isComponentInstalled(ZEPPELIN)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(ZEPPELIN));
                break;
            }
            Zeppelin_action(STOP);
            break;
        case CliMsg_Zeppelin_Start:
            if (!isComponentInstalled(ZEPPELIN)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(ZEPPELIN));
                break;
            }
            Zeppelin_action(START);
            break;
        case CliMsg_Zeppelin_Restart:
            if (!isComponentInstalled(ZEPPELIN)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(ZEPPELIN));
                break;
            }
            Zeppelin_action(RESTART);
            break;
        case CliMsg_Zeppelin_Uninstall:
            if (!isComponentInstalled(ZEPPELIN)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(ZEPPELIN));
                break;
            }
            uninstall_Zeppelin();
            break;
        case CliMsg_Zeppelin_Install_Version:
            if (isComponentInstalled(ZEPPELIN)){
                PRINTF(global_client_socket, "%s is already installed.\n", component_to_string(ZEPPELIN));
                break;
            }
            install_Zeppelin(result[0],result[1]);
            configure_target_component(ZEPPELIN);
            break;
        case CliMsg_Zeppelin_Install:
            if (isComponentInstalled(ZEPPELIN)){
                PRINTF(global_client_socket, "%s is already installed.\n", component_to_string(ZEPPELIN));
                break;
            }
            install_Zeppelin(NULL, NULL);
            configure_target_component(ZEPPELIN);
            break;
        case CliMsg_Zeppelin:
            if (!isComponentInstalled(ZEPPELIN)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(ZEPPELIN));
                break;
            }
            PRINTF(global_client_socket, report_zeppelin());
            break;

            /* ================= Configuration Commands =============== */
        case CliMsg_Hdfs_Configure:
            if (!isComponentInstalled(HDFS)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(HDFS));
                break;
            }
            ValidationResult validationresult = validateHdfsConfigParam(result[0], result[1]);
            if (!handleValidationResult(validationresult))
                break;
            ConfigResult *hdfsResult= find_hdfs_config(result[0]);
            if (hdfsResult == NULL)
                FPRINTF(global_client_socket,"configuration parameter not supported yet");
            ConfigStatus hdfsStatus =  modify_hdfs_config(hdfsResult->canonical_name,hdfsResult->value,hdfsResult->config_file);
            handle_result(hdfsStatus, hdfsResult->canonical_name,result[1],hdfsResult->config_file);
            break;
        case CliMsg_HBase_Configure:
            if (!isComponentInstalled(HBASE)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(HBASE));
                break;
            }
            ValidationResult validationHbase = validateHBaseConfigParam(result[0], result[1]);
            if (!handleValidationResult(validationHbase))
                break;
            ConfigResult *hbaseResult= process_hbase_config(result[0],result[1]);
            if (hbaseResult == NULL)
                FPRINTF(global_client_socket,"configuration parameter not supported yet");
            ConfigStatus hbaseStatus =  update_hbase_config(hbaseResult->canonical_name, hbaseResult->value, hbaseResult->config_file);
            handle_result(hbaseStatus, hbaseResult->canonical_name, hbaseResult->value, hbaseResult->config_file);
            break;
        case CliMsg_Spark_Configure:
            if (!isComponentInstalled(SPARK)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(SPARK));
                break;
            }
            ValidationResult validationSpark = validateSparkConfigParam(result[0], result[1]);
            if (!handleValidationResult(validationSpark))
                break;
            ConfigResult *sparkResult= get_spark_config(result[0],result[1]);
            if (sparkResult == NULL)
                FPRINTF(global_client_socket,"configuration parameter not supported yet");
            ConfigStatus sparkStatus = update_spark_config(sparkResult->canonical_name, sparkResult->value, sparkResult->config_file);
            handle_result(sparkStatus, sparkResult->canonical_name, sparkResult->value, sparkResult->config_file);
            break;
        case CliMsg_Kafka_Configure:
            if (!isComponentInstalled(KAFKA)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(KAFKA));
                break;
            }
            ValidationResult validationKafka = validateKafkaConfigParam(result[0], result[1]);
            if (!handleValidationResult(validationKafka))
                break;
            ConfigResult *kafkaResult = validate_kafka_config_param(result[0],result[1]);
            if (kafkaResult == NULL)
                PRINTF(global_client_socket,"configuration parameter not supported yet");
            ConfigStatus kafkaStatus = modify_kafka_config(kafkaResult->canonical_name,kafkaResult->value,kafkaResult->config_file);
            handle_result(kafkaStatus, kafkaResult->canonical_name,kafkaResult->value,kafkaResult->config_file);
            break;
        case CliMsg_Flink_Configure:
            if (!isComponentInstalled(FLINK)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(FLINK));
                break;
            }
            ValidationResult validationflink = validateFlinkConfigParam(result[0], result[1]);
            if (!handleValidationResult(validationflink))
                break;
            ConfigResult *flinkResult = set_flink_config(result[0],result[1]);
            if (flinkResult ==NULL)
                FPRINTF(global_client_socket,"configuration parameter not supported yet");
            ConfigStatus flinkStatus = update_flink_config(flinkResult->canonical_name,flinkResult->value, flinkResult->config_file);
            handle_result(flinkStatus, flinkResult->canonical_name,flinkResult->value, flinkResult->config_file);
            break;
        case CliMsg_ZooKeeper_Configure:
            if (!isComponentInstalled(ZOOKEEPER)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(ZOOKEEPER));
                break;
            }
            ValidationResult validationZookeeper = validateZooKeeperConfigParam(result[0], result[1]);
            if (!handleValidationResult(validationZookeeper))
                break;
            ConfigResult *zookeperResult = parse_zookeeper_param(result[0],result[1]);
            if (zookeperResult == NULL)
                FPRINTF(global_client_socket,"configuration parameter not supported yet");
            ConfigStatus zookeeperStatus = modify_zookeeper_config(zookeperResult->canonical_name, zookeperResult->value, zookeperResult->config_file);
            handle_result(zookeeperStatus, zookeperResult->canonical_name, zookeperResult->value, zookeperResult->config_file);
            break;
        case CliMsg_Storm_Configure:
            if (!isComponentInstalled(STORM)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(STORM));
                break;
            }
            ValidationResult validationStorm = validateStormConfigParam(result[0], result[1]);
            if (!handleValidationResult(validationStorm))
                break;
            ConfigResult *conf = validate_storm_config_param(result[0],result[1]);
            if (conf == NULL)
                FPRINTF(global_client_socket,"configuration parameter not supported yet");
            ConfigStatus stormStatus = modify_storm_config(conf->canonical_name, conf->value, conf->config_file);
            handle_result(stormStatus, conf->canonical_name, conf->value, conf->config_file);
            break;
        case CliMsg_Hive_Configure:
            if (!isComponentInstalled(HIVE)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(HIVE));
                break;
            }
            ValidationResult validationHive = validateHiveConfigParam(result[0], result[1]);
            if (!handleValidationResult(validationHive))
                break;
            ConfigResult *hiveConf = process_hive_parameter(result[0],result[1]);
            if (hiveConf == NULL)
                FPRINTF(global_client_socket,"configuration parameter not supported yet");
            ConfigStatus hiveStatus = modify_hive_config(hiveConf->canonical_name, hiveConf->value, hiveConf->config_file);
            handle_result(hiveStatus, hiveConf->canonical_name, hiveConf->value, hiveConf->config_file);
            break;
        case CliMsg_Pig_Configure:
            if (!isComponentInstalled(PIG)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(PIG));
                break;
            }
            ValidationResult validationPig = validatePigConfigParam(result[0], result[1]);
            if (!handleValidationResult(validationPig))
                break;
            ConfigResult *pigConf = validate_pig_config_param(result[0],result[1]);
            if (pigConf == NULL)
                FPRINTF(global_client_socket,"configuration parameter not supported yet");
            ConfigStatus pigStatus = update_pig_config(pigConf->canonical_name, pigConf->value);
            handle_result(pigStatus, pigConf->canonical_name, pigConf->value,pigConf->config_file);
            break;
            //  case CliMsg_Presto_Configure:
            //    return modify_oozie_config(result[0],result[1]);
            //case CliMsg_Atlas_Configure:
            //  return modify_oozie_config(result[0],result[1]);
        case CliMsg_Ranger_Configure:
            if (!isComponentInstalled(RANGER)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(RANGER));
                break;
            }
            ConfigResult *rangerConf = process_zeppelin_config_param(result[0],result[1]);
            if (rangerConf == NULL)
                FPRINTF(global_client_socket,"configuration parameter not supported yet");
            ConfigStatus rangerStatus =  set_ranger_config(rangerConf->canonical_name, rangerConf->value, rangerConf->config_file);
            handle_result(rangerStatus, rangerConf->canonical_name, rangerConf->value, rangerConf->config_file);
            break;
        case CliMsg_Livy_Configure:
            if (!isComponentInstalled(LIVY)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(LIVY));
                break;
            }
            ValidationResult validationLivy = validateLivyConfigParam(result[0], result[1]);
            if (!handleValidationResult(validationLivy))
                break;
            ConfigResult *livyConf = parse_livy_config_param(result[0],result[1]);
            if (livyConf == NULL)
                FPRINTF(global_client_socket,"configuration parameter not supported yet");
            ConfigStatus livyStatus = set_livy_config(livyConf->canonical_name, livyConf->value,livyConf->config_file);
            handle_result(livyStatus, livyConf->canonical_name, livyConf->value,livyConf->config_file);
            break;
            // case CliMsg_Phoenix_Configure:
            //   ConfigStatus phoenixStatus = update_phoenix_config(result[0],result[1]);
            // handle_result(phoenixStatus);
            // break;
        case CliMsg_Solr_Configure:
            if (!isComponentInstalled(SOLR)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(SOLR));
                break;
            }
            ValidationResult validationSolr = validateSolrConfigParam(result[0], result[1]);
            if (!handleValidationResult(validationSolr))
                break;
            ConfigResult *solrConf = validate_solr_parameter(result[0],result[1]);
            if (solrConf == NULL)
                FPRINTF(global_client_socket,"configuration parameter not supported yet");
            ConfigStatus solrStatus =  update_solr_config(solrConf->canonical_name, solrConf->value, solrConf->config_file);
            handle_result(solrStatus, solrConf->canonical_name, solrConf->value, solrConf->config_file);
            break;
        case CliMsg_Zeppelin_Configure:
            if (!isComponentInstalled(ZEPPELIN)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(ZEPPELIN));
                break;
            }
            ValidationResult validationZeppelin = validateZeppelinConfigParam(result[0], result[1]);
            if (!handleValidationResult(validationZeppelin))
                break;
            ConfigResult *zeppelinConf = process_zeppelin_config_param(result[0],result[1]);
            if (zeppelinConf == NULL)
                FPRINTF(global_client_socket,"configuration parameter not supported yet");
            ConfigStatus zeppStatus =  set_zeppelin_config(zeppelinConf->config_file , zeppelinConf->canonical_name, zeppelinConf->value);
            handle_result(zeppStatus, zeppelinConf->canonical_name, zeppelinConf->value,zeppelinConf->config_file);
            break;
        case CliMsg_Tez_Configure:
            if (!isComponentInstalled(TEZ)){
                FPRINTF(global_client_socket, "%s is not installed.\n", component_to_string(TEZ));
                break;
            }
            ValidationResult validationTez = validateTezConfigParam(result[0], result[1]);
            if (!handleValidationResult(validationTez))
                break;
            ConfigResult *tezConf = parse_tez_config_param(result[0],result[1]);
            if (tezConf == NULL)
                FPRINTF(global_client_socket,"configuration parameter not supported yet");
            ConfigStatus tezStatus =   modify_tez_config(tezConf->canonical_name, tezConf->value, "tez-site.xml");
            handle_result(tezStatus, tezConf->canonical_name, tezConf->value, "tez-site.xml");
            break;
            /* =================== Error Handling ===================== */
        default:
            FPRINTF(global_client_socket, "Unknown action code: 0x%02X\n", (unsigned char)action_code);
            // send_error(client_socket, "Invalid command");
            break;
        }
    }
}
