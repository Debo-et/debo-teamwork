/*-------------------------------------------------------------------------
 *
 * comm.c
 *	  Communication functions between the Frontend and the Backend
 *
 * These routines handle the low-level details of communication between
 * frontend and backend.  They just shove data across the communication
 * channel, and are ignorant of the semantics of the data.
 *
 * To emit an outgoing message, use the routines in format.c to construct
 * the message in a buffer and then emit it in one call to putmessage.
 * There are no functions to send raw bytes or partial messages; this
 * ensures that the channel will not be clogged by an incomplete message if
 * execution is aborted by partway through the message.
 *
 *-------------------------------------------------------------------------
 */

/*------------------------
 * INTERFACE ROUTINES
 *
 * setup/teardown:
 *		ListenServerPort	- Open  server port
 *		AcceptConnection	- Accept new connection with client
 *
 * low-level I/O:
 *		getbytes		- get a known number of bytes from connection
 *		getmessage	- get a message with length word from connection
 *		getbyte		- get next byte from connection
 *		peekbyte		- peek at next byte from connection
 *		flush		- flush pending output
 *		flush_if_writable - flush pending output if writable without blocking
 *
 *------------------------
 */

#ifdef HAVE_POLL_H
#include <poll.h>
#endif
#include <signal.h>
#include <fcntl.h>
#include <grp.h>
#include <unistd.h>
#include <sys/file.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <utime.h>
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>

#include "ip.h"
#include "connutil.h"
#include "comm.h"


#define ntoh32(x) (x)
#define hton32(x) (x)

#define STATUS_OK                               (0)
#define STATUS_ERROR                    (-1)
#define STATUS_EOF                              (-2)



#define closesocket close

/* only GCC supports the unused attribute */
#ifdef __GNUC__
#define db_attribute_unused() __attribute__((unused))
#else
#define db_attribute_unused()
#endif
typedef int32_t int32;


#ifdef USE_ASSERT_CHECKING
#define DB_USED_FOR_ASSERTS_ONLY
#else
#define DB_USED_FOR_ASSERTS_ONLY db_attribute_unused()
#endif

/* GCC supports format attributes */
#if defined(__GNUC__)
#define db_attribute_format_arg(a) __attribute__((format_arg(a)))
#define db_attribute_printf(f,a) __attribute__((format(DB_PRINTF_ATTRIBUTE, f, a)))
#else
#define db_attribute_format_arg(a)
#define db_attribute_printf(f,a)
#endif


/*
 * Cope with the various platform-specific ways to spell TCP keepalive socket
 * options.  This doesn't cover Windows, which as usual does its own thing.
 */
#if defined(TCP_KEEPIDLE)
/* TCP_KEEPIDLE is the name of this option on Linux and *BSD */
#define DB_TCP_KEEPALIVE_IDLE TCP_KEEPIDLE
#define DB_TCP_KEEPALIVE_IDLE_STR "TCP_KEEPIDLE"
#elif defined(TCP_KEEPALIVE_THRESHOLD)
/* TCP_KEEPALIVE_THRESHOLD is the name of this option on Solaris >= 11 */
#define DB_TCP_KEEPALIVE_IDLE TCP_KEEPALIVE_THRESHOLD
#define DB_TCP_KEEPALIVE_IDLE_STR "TCP_KEEPALIVE_THRESHOLD"
#elif defined(TCP_KEEPALIVE) && defined(__darwin__)
/* TCP_KEEPALIVE is the name of this option on macOS */
/* Caution: Solaris has this symbol but it means something different */
#define DB_TCP_KEEPALIVE_IDLE TCP_KEEPALIVE
#define DB_TCP_KEEPALIVE_IDLE_STR "TCP_KEEPALIVE"
#endif
int                     MaxConnections = 100;
#define DBINVALID_SOCKET (-1)
struct Port *MyProcPort;
volatile sig_atomic_t ClientConnectionLost = false;
volatile sig_atomic_t InterruptPending = false;
int                     tcp_keepalives_idle;
int                     tcp_keepalives_interval;
int                     tcp_keepalives_count;
int                     tcp_user_timeout;
/*
 * Configuration options
 */
int			Unix_socket_permissions;
char	   *Unix_socket_group;

#define MAXPGPATH               1024
/*
 * Buffers for low-level I/O.
 *
 * The receive buffer is fixed size. Send buffer is usually 8k, but can be
 * enlarged by putmessage_noblock() if the message doesn't fit otherwise.
 */

#define SEND_BUFFER_SIZE 8192
#define RECV_BUFFER_SIZE 8192

static char *DbSendBuffer;
static int	DbSendBufferSize;	/* Size send buffer */
static size_t DbSendPointer;	/* Next index to store a byte in PqSendBuffer */
static size_t DbSendStart;		/* Next index to send a byte in PqSendBuffer */

static char DbRecvBuffer[RECV_BUFFER_SIZE];
static int	DbRecvPointer;		/* Next index to read a byte from DbRecvBuffer */
static int	DbRecvLength;		/* End of data available in DbRecvBuffer */

/*
 * Message status
 */
static bool DbCommBusy;			/* busy sending data to the client */
static bool DbCommReadingMsg;	/* in the middle of reading a message */


/* Internal functions */
static void socket_comm_reset(void);
static bool socket_is_send_pending(void);
static void socket_putmessage_noblock(char msgtype, const char *s, size_t len);
db_noinline int internal_flush_buffer(ClientSocket *client_sock ,const char *buf, size_t *start,
                                      size_t *end);
static int	Setup_AF_UNIX(const char *sock_path);

static const DBcommMethods DbCommSocketMethods = {
    .comm_reset = socket_comm_reset,
    .is_send_pending = socket_is_send_pending,
    .putmessage_noblock = socket_putmessage_noblock
};

const DBcommMethods *DbCommMethods = &DbCommSocketMethods;

ssize_t
secure_raw_read(ClientSocket *client_sock ,void *ptr, size_t len)
{
    size_t         n;

    /* Read from the "unread" buffered data first. c.f. libpq-be.h */
    //       if (port->raw_buf_remaining > 0)
    //     {
    /* consume up to len bytes from the raw_buf */
    //           if (len > (size_t)port->raw_buf_remaining)
    //                 len = port->raw_buf_remaining;
    //       Assert(port->raw_buf);
    //     memcpy(ptr, port->raw_buf + port->raw_buf_consumed, len);
    //   port->raw_buf_consumed += len;
    // port->raw_buf_remaining -= len;
    //return len;
    //}

    /*
     * Try to read from the socket without blocking. If it succeeds we're
     * done, otherwise we'll wait for the socket using the latch mechanism.
     */
#ifdef WIN32
    pgwin32_noblock = true;
#endif
    n = recv(client_sock->sock, ptr, len, 0);
#ifdef WIN32
    pgwin32_noblock = false;
#endif

    return n;
}

ssize_t
secure_raw_write(ClientSocket *client_sock, const void *ptr, size_t len)
{
    ssize_t         n;

#ifdef WIN32
    pgwin32_noblock = true;
#endif
    n = send(client_sock->sock, ptr, len, 0);
#ifdef WIN32
    pgwin32_noblock = false;
#endif

    return n;
}




/* --------------------------------
 *		init - initialize
 * --------------------------------
 */
Port *
init(ClientSocket *client_sock)
{
    Port	   *port;
    int			socket_pos DB_USED_FOR_ASSERTS_ONLY;
    int			latch_pos DB_USED_FOR_ASSERTS_ONLY;

    /* allocate the Port struct and copy the ClientSocket contents to it */
    port = malloc(sizeof(Port));
    port->sock = client_sock->sock;
    memcpy(&port->raddr.addr, &client_sock->raddr.addr, client_sock->raddr.salen);
    port->raddr.salen = client_sock->raddr.salen;

    /* fill in the server (local) address */
    port->laddr.salen = sizeof(port->laddr.addr);
    if (getsockname(port->sock,
                    (struct sockaddr *) &port->laddr.addr,
                    &port->laddr.salen) < 0)
    {
        fprintf(stderr, "%s() failed: %m", "getsockname");
    }

    /* select NODELAY and KEEPALIVE options if it's a TCP connection */
    if (port->laddr.addr.ss_family != AF_UNIX)
    {
        int			on;
#ifdef WIN32
        int			oldopt;
        int			optlen;
        int			newopt;
#endif

#ifdef	TCP_NODELAY
        on = 1;
        if (setsockopt(port->sock, IPPROTO_TCP, TCP_NODELAY,
                       (char *) &on, sizeof(on)) < 0)
        {
            fprintf(stderr, "%s(%s) failed: %m", "setsockopt", "TCP_NODELAY");
        }
#endif
        on = 1;
        if (setsockopt(port->sock, SOL_SOCKET, SO_KEEPALIVE,
                       (char *) &on, sizeof(on)) < 0)
        {
            fprintf(stderr, "%s(%s) failed: %m", "setsockopt", "SO_KEEPALIVE");
        }

#ifdef WIN32

        /*
         * This is a Win32 socket optimization.  The OS send buffer should be
         * large enough to send the whole  buffer in one go, or
         * performance suffers.  The  send buffer can be enlarged if a
         * very large message needs to be sent, but we won't attempt to
         * enlarge the OS buffer if that happens, so somewhat arbitrarily
         * ensure that the OS buffer is at least SEND_BUFFER_SIZE * 4.
         * (That's 32kB with the current default).
         *
         * The default OS buffer size used to be 8kB in earlier Windows
         * versions, but was raised to 64kB in Windows 2012.  So it shouldn't
         * be necessary to change it in later versions anymore.  Changing it
         * unnecessarily can even reduce performance, because setting
         * SO_SNDBUF in the application disables the "dynamic send buffering"
         * feature that was introduced in Windows 7.  So before fiddling with
         * SO_SNDBUF, check if the current buffer size is already large enough
         * and only increase it if necessary.
         *
         * See https://support.microsoft.com/kb/823764/EN-US/ and
         * https://msdn.microsoft.com/en-us/library/bb736549%28v=vs.85%29.aspx
         */
        optlen = sizeof(oldopt);
        if (getsockopt(port->sock, SOL_SOCKET, SO_SNDBUF, (char *) &oldopt,
                       &optlen) < 0)
        {
            fprintf(stderr, "%s(%s) failed: %m", "getsockopt", "SO_SNDBUF");
        }
        newopt = SEND_BUFFER_SIZE * 4;
        if (oldopt < newopt)
        {
            if (setsockopt(port->sock, SOL_SOCKET, SO_SNDBUF, (char *) &newopt,
                           sizeof(newopt)) < 0)
            {
                fprintf(stderr, "%s(%s) failed: %m", "setsockopt", "SO_SNDBUF");
            }
        }
#endif

        /*
         * Also apply the current keepalive parameters.  If we fail to set a
         * parameter, don't error out, because these aren't universally
         * supported.
         */
        (void) setkeepalivesidle(tcp_keepalives_idle, port);
        (void) setkeepalivesinterval(tcp_keepalives_interval, port);
        (void) setkeepalivescount(tcp_keepalives_count, port);
        (void) settcpusertimeout(tcp_user_timeout, port);
    }

    /* initialize state variables */
    DbSendBufferSize = SEND_BUFFER_SIZE;
    DbSendBuffer = malloc(DbSendBufferSize);
    DbSendPointer = DbSendStart = DbRecvPointer = DbRecvLength = 0;
    DbCommBusy = false;
    DbCommReadingMsg = false;

    /* set up process-exit hook to close the socket */
    //on_proc_exit(socket_close, 0);

    /*
     * In backends (as soon as forked) we operate the underlying socket in
     * nonblocking mode and use latches to implement blocking semantics if
     * needed. That allows us to provide safely interruptible reads and
     * writes.
     */
#ifndef WIN32
    if (!fcntl(port->sock, F_GETFL))
        printf("could not set socket to nonblocking mode: %m");
#endif

#ifndef WIN32

    /* Don't give the socket to any subprograms we execute. */
    if (fcntl(port->sock, F_SETFD, FD_CLOEXEC) < 0)
        printf("fcntl(F_SETFD) failed on socket: %m");
#endif

    //FeBeWaitSet = CreateWaitEventSet(NULL, FeBeWaitSetNEvents);
    //socket_pos = AddWaitEventToSet(FeBeWaitSet, WL_SOCKET_WRITEABLE,
    //							   port->sock, NULL, NULL);
    //latch_pos = AddWaitEventToSet(FeBeWaitSet, WL_LATCH_SET, PGINVALID_SOCKET,
    //							  MyLatch, NULL);
    //AddWaitEventToSet(FeBeWaitSet, WL_POSTMASTER_DEATH, PGINVALID_SOCKET,
    //				  NULL, NULL);

    /*
     * The event positions match the order we added them, but let's sanity
     * check them to be sure.
     */
    //Assert(socket_pos == FeBeWaitSetSocketPos);
    //Assert(latch_pos == FeBeWaitSetLatchPos);

    return port;
}

/* --------------------------------
 *
 * This is called from error recovery at the outer idle loop.  It's
 * just to get us out of trouble if we somehow manage to log() from
 * inside a comm.c routine (which ideally will never happen, but...)
 * --------------------------------
 */
static void
socket_comm_reset(void)
{
    /* Do not throw away pending data, but do reset the busy flag */
    DbCommBusy = false;
}



/*
 * ListenServerPort -- open a "listening" port to accept connections.
 *
 * family should be AF_UNIX or AF_UNSPEC; portNumber is the port number.
 * For AF_UNIX ports, hostName should be NULL and unixSocketDir must be
 * specified.  For TCP ports, hostName is either NULL for all interfaces or
 * the interface to listen on, and unixSocketDir is ignored (can be NULL).
 *
 * Successfully opened sockets are appended to the ListenSockets[] array.  On
 * entry, *NumListenSockets holds the number of elements currently in the
 * array, and it is updated to reflect the opened sockets.  MaxListen is the
 * allocated size of the array.
 *
 * RETURNS: STATUS_OK or STATUS_ERROR
 */
int
ListenServerPort(int family, const char *hostName, unsigned short portNumber,
                 int ListenSockets[], int *NumListenSockets, int MaxListen)
{
    int	fd;
    int			err;
    int			maxconn;
    int			ret;
    char		portNumberStr[32];
    const char *familyDesc;
    char		familyDescBuf[64];
    const char *addrDesc;
    char		addrBuf[NI_MAXHOST];
    char	   *service;
    struct addrinfo *addrs = NULL,
                    *addr;
    struct addrinfo hint;
    int			added = 0;
    char		unixSocketPath[MAXPGPATH];
#if !defined(WIN32) || defined(IPV6_V6ONLY)
    int			one = 1;
#endif

    /* Initialize hint structure */
    MemSet(&hint, 0, sizeof(hint));
    hint.ai_family = family;
    hint.ai_flags = AI_PASSIVE;
    hint.ai_socktype = SOCK_STREAM;

    snprintf(portNumberStr, sizeof(portNumberStr), "%d", portNumber);
    service = portNumberStr;

    ret = getaddrinfo_all(hostName, service, &hint, &addrs);
    if (ret || !addrs)
    {
        if (hostName)
            fprintf(stderr, "could not translate host name \"%s\", service \"%s\" to address: %s",
                    hostName, service, gai_strerror(ret));
        else
            fprintf(stderr, "could not translate service \"%s\" to address: %s",
                    service, gai_strerror(ret));
        if (addrs)
            freeaddrinfo_all(hint.ai_family, addrs);
        return STATUS_ERROR;
    }

    for (addr = addrs; addr; addr = addr->ai_next)
    {
        if (family != AF_UNIX && addr->ai_family == AF_UNIX)
        {
            /*
             * Only set up a unix domain socket when they really asked for it.
             * The service/port is different in that case.
             */
            continue;
        }

        /* See if there is still room to add 1 more socket. */
        if (*NumListenSockets == MaxListen)
        {
            fprintf(stderr, "could not bind to all requested addresses: MAXLISTEN (%d) exceeded",
                    MaxListen);
            break;
        }

        /* set up address family name for log messages */
        switch (addr->ai_family)
        {
        case AF_INET:
            familyDesc = ("IPv4");
            break;
        case AF_INET6:
            familyDesc = ("IPv6");
            break;
        case AF_UNIX:
            familyDesc = ("Unix");
            break;
        default:
            snprintf(familyDescBuf, sizeof(familyDescBuf),
                     ("unrecognized address family %d"),
                     addr->ai_family);
            familyDesc = familyDescBuf;
            break;
        }

        /* set up text form of address for log messages */
        if (addr->ai_family == AF_UNIX)
            addrDesc = unixSocketPath;
        else
        {
            getnameinfo_all((const struct sockaddr_storage *) addr->ai_addr,
                            addr->ai_addrlen,
                            addrBuf, sizeof(addrBuf),
                            NULL, 0,
                            NI_NUMERICHOST);
            addrDesc = addrBuf;
        }

        if ((fd = socket(addr->ai_family, SOCK_STREAM, 0)) == DBINVALID_SOCKET)
        {
            fprintf(stderr, "could not create %s socket for address \"%s\": %m",
                    familyDesc, addrDesc);
            continue;
        }

#ifndef WIN32
        /* Don't give the listen socket to any subprograms we execute. */
        if (fcntl(fd, F_SETFD, FD_CLOEXEC) < 0)
            printf("fcntl(F_SETFD) failed on socket: %m");

        /*
         * Without the SO_REUSEADDR flag, a new  can't be started
         * right away after a stop or crash, giving "address already in use"
         * error on TCP ports.
         *
         * On win32, however, this behavior only happens if the
         * SO_EXCLUSIVEADDRUSE is set. With SO_REUSEADDR, win32 allows
         * multiple servers to listen on the same address, resulting in
         * unpredictable behavior. With no flags at all, win32 behaves as Unix
         * with SO_REUSEADDR.
         */
        if (addr->ai_family != AF_UNIX)
        {
            if ((setsockopt(fd, SOL_SOCKET, SO_REUSEADDR,
                            (char *) &one, sizeof(one))) == -1)
            {
                fprintf(stderr, "%s(%s) failed for %s address \"%s\": %m",
                        "setsockopt", "SO_REUSEADDR",
                        familyDesc, addrDesc);
                closesocket(fd);
                continue;
            }
        }
#endif

#ifdef IPV6_V6ONLY
        if (addr->ai_family == AF_INET6)
        {
            if (setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY,
                           (char *) &one, sizeof(one)) == -1)
            {
                fprintf(stderr, "%s(%s) failed for %s address \"%s\": %m",
                        "setsockopt", "IPV6_V6ONLY",
                        familyDesc, addrDesc);
                closesocket(fd);
                continue;
            }
        }
#endif

        /*
         * Note: This might fail on some OS's, like Linux older than
         * 2.4.21-pre3, that don't have the IPV6_V6ONLY socket option, and map
         * ipv4 addresses to ipv6.  It will show ::ffff:ipv4 for all ipv4
         * connections.
         */
        err = bind(fd, addr->ai_addr, addr->ai_addrlen);
        if (err < 0)
        {
            fprintf(stderr, "Is another debo already running on port %d?",
                    (int) portNumber);
            closesocket(fd);
            continue;
        }

        if (addr->ai_family == AF_UNIX)
        {
            if (Setup_AF_UNIX(service) != STATUS_OK)
            {
                closesocket(fd);
                break;
            }
        }

        /*
         * Select appropriate accept-queue length limit.  It seems reasonable
         * to use a value similar to the maximum number of child processes
         * that the postmaster will permit.
         */
        maxconn = MaxConnections * 2;

        err = listen(fd, maxconn);
        if (err < 0)
        {
            fprintf(stderr, "could not listen on %s address \"%s\": %m",
                    familyDesc, addrDesc);
            closesocket(fd);
            continue;
        }

        if (addr->ai_family == AF_UNIX)
            fprintf(stderr, "listening on Unix socket \"%s\"",
                    addrDesc);
        else
            fprintf(stderr, "listening on %s address \"%s\", port %d",
                    familyDesc, addrDesc, (int) portNumber);

        ListenSockets[*NumListenSockets] = fd;
        (*NumListenSockets)++;
        added++;
    }

    freeaddrinfo_all(hint.ai_family, addrs);

    if (!added)
        return STATUS_ERROR;

    return STATUS_OK;
}



/*
 * Setup_AF_UNIX -- configure unix socket permissions
 */
static int
Setup_AF_UNIX(const char *sock_path)
{
    /* no file system permissions for abstract sockets */
    if (sock_path[0] == '@')
        return STATUS_OK;

    /*
     * Fix socket ownership/permission if requested.  Note we must do this
     * before we listen() to avoid a window where unwanted connections could
     * get accepted.
     */
    Assert(Unix_socket_group);
    if (Unix_socket_group[0] != '\0')
    {
#ifdef WIN32
        fprintf(stderr, "configuration item \"unix_socket_group\" is not supported on this platform");
#else
        char	   *endptr;
        unsigned long val;
        gid_t		gid;

        val = strtoul(Unix_socket_group, &endptr, 10);
        if (*endptr == '\0')
        {						/* numeric group id */
            gid = val;
        }
        else
        {						/* convert group name to id */
            struct group *gr;

            gr = getgrnam(Unix_socket_group);
            if (!gr)
            {
                fprintf(stderr, "group \"%s\" does not exist",
                        Unix_socket_group);
                return STATUS_ERROR;
            }
            gid = gr->gr_gid;
        }
        if (chown(sock_path, -1, gid) == -1)
        {
            fprintf(stderr, "could not set group of file \"%s\": %m",
                    sock_path);
            return STATUS_ERROR;
        }
#endif
    }

    if (chmod(sock_path, Unix_socket_permissions) == -1)
    {
        fprintf(stderr, "could not set permissions of file \"%s\": %m",
                sock_path);
        return STATUS_ERROR;
    }
    return STATUS_OK;
}
/*
 * Put socket into nonblock mode.
 * Returns true on success, false on failure.
 */
bool
set_noblock(int sock)
{
#if !defined(WIN32)
    int                     flags;

    flags = fcntl(sock, F_GETFL);
    if (flags < 0)
        return false;
    if (fcntl(sock, F_SETFL, (flags | O_NONBLOCK)) == -1)
        return false;
    return true;
#else
    unsigned long ioctlsocket_ret = 1;

    /* Returns non-0 on failure, while fcntl() returns -1 on failure */
    return (ioctlsocket(sock, FIONBIO, &ioctlsocket_ret) == 0);
#endif
}


/*
 * Accept connection and configure non-blocking mode
 */
int
AcceptConnection(int server_fd, ClientSocket *client_sock)
{
    client_sock->raddr.salen = sizeof(client_sock->raddr.addr);
    client_sock->sock = accept(server_fd,
                               (struct sockaddr *) &client_sock->raddr.addr,
                               &client_sock->raddr.salen);

    if (client_sock->sock == DBINVALID_SOCKET)
    {
        printf("could not accept connection: ");
        return STATUS_ERROR;
    }

    // Set non-blocking mode for the new client socket
    if (!set_noblock(client_sock->sock))
    {
        closesocket(client_sock->sock);
        return STATUS_ERROR;
    }

    return STATUS_OK;
}


/* --------------------------------
 *		recvbuf - load some bytes into the input buffer
 *
 *		returns 0 if OK, EOF if trouble
 * --------------------------------
 */
static int
recvbuf(ClientSocket *client_sock)
{
    if (DbRecvPointer > 0)
    {
        if (DbRecvLength > DbRecvPointer)
        {
            /* still some unread data, left-justify it in the buffer */
            memmove(DbRecvBuffer, DbRecvBuffer + DbRecvPointer,
                    DbRecvLength - DbRecvPointer);
            DbRecvLength -= DbRecvPointer;
            DbRecvPointer = 0;
        }
        else
            DbRecvLength = DbRecvPointer = 0;
    }

    /* Can fill buffer from DbRecvLength and upwards */
    for (;;)
    {
        int			r;

        errno = 0;

        r = be_gssapi_read(client_sock , DbRecvBuffer + DbRecvLength,
                           RECV_BUFFER_SIZE - DbRecvLength);

        if (r < 0)
        {
            if (errno == EINTR)
                continue;		/* Ok if interrupted */

            /*
             * Careful: an ereport() that tries to write to the client would
             * cause recursion to here, leading to stack overflow and core
             * dump!  This message must go *only* to the postmaster log.
             *
             * If errno is zero, assume it's EOF and let the caller complain.
             */
            if (errno != 0)
                printf("could not receive data from client: %m");
            return EOF;
        }
        if (r == 0)
        {
            /*
             * EOF detected.  We used to write a log message here, but it's
             * better to expect the ultimate caller to do that.
             */
            return EOF;
        }
        /* r contains number of bytes read, so just incr length */
        DbRecvLength += r;
        return 0;
    }
}

/* --------------------------------
 *		getbyte	- get a single byte from connection, or return EOF
 * --------------------------------
 */
int
getbyte(ClientSocket *client_sock)
{
    Assert(DbCommReadingMsg);

    while (DbRecvPointer >= DbRecvLength)
    {
        if (recvbuf(client_sock))		/* If nothing in buffer, then recv some */
            return EOF;			/* Failed to recv data */
    }
    return (unsigned char) DbRecvBuffer[DbRecvPointer++];
}




/* --------------------------------
 *		buffer_remaining_data	- return number of bytes in receive buffer
 *
 * This will *not* attempt to read more data. And reading up to that number of
 * bytes should not cause reading any more data either.
 * --------------------------------
 */
ssize_t
buffer_remaining_data(void)
{
    Assert(DbRecvLength >= DbRecvPointer);
    return (DbRecvLength - DbRecvPointer);
}


/* --------------------------------
 *		startmsgread - begin reading a message from the client.
 *
 *		This must be called before any of the get* functions.
 * --------------------------------
 */
void
startmsgread(void)
{
    /*
     * There shouldn't be a read active already, but let's check just to be
     * sure.
     */
    if (DbCommReadingMsg)
        printf("terminating connection because protocol synchronization was lost");

    DbCommReadingMsg = true;
}


/* --------------------------------
 *		endmsgread	- finish reading message.
 *
 *		This must be called after reading a message with getbytes()
 *		and friends, to indicate that we have read the whole message.
 *		getmessage() does this implicitly.
 * --------------------------------
 */
void
endmsgread(void)
{
    Assert(DbCommReadingMsg);

    DbCommReadingMsg = false;
}

/* --------------------------------
 *		is_reading_msg - are we currently reading a message?
 *
 * This is used in error recovery at the outer idle loop to detect if we have
 * lost protocol sync, and need to terminate the connection. startmsgread()
 * will check for that too, but it's nicer to detect it earlier.
 * --------------------------------
 */
bool
is_reading_msg(void)
{
    return DbCommReadingMsg;
}
/* --------------------------------
 *              getbytes             - get a known number of bytes from connection
 *
 *              returns 0 if OK, EOF if trouble
 * --------------------------------
 */
int
getbytes(char *s, size_t len, ClientSocket *client_sock )
{
    size_t          amount;

    Assert(DbCommReadingMsg);

    while (len > 0)
    {
        while (DbRecvPointer >= DbRecvLength)
        {
            if (recvbuf(client_sock))       /* If nothing in buffer, then recv some */
                return EOF;             /* Failed to recv data */
        }
        amount = DbRecvLength - DbRecvPointer;
        if (amount > len)
            amount = len;
        memcpy(s, DbRecvBuffer + DbRecvPointer, amount);
        DbRecvPointer += amount;
        s += amount;
        len -= amount;
    }
    return 0;
}

/* --------------------------------
 *		getmessage	- get a message with length word from connection
 *
 *		The return value is placed in an expansible StringInfo, which has
 *		already been initialized by the caller.
 *		Only the message body is placed in the StringInfo; the length word
 *		is removed.  Also, s->cursor is initialized to zero for convenience
 *		in scanning the message contents.
 *
 *		maxlen is the upper limit on the length of the
 *		message we are willing to accept.  We abort the connection (by
 *		returning EOF) if client tries to send more than that.
 *
 *		returns 0 if OK, EOF if trouble
 * --------------------------------
 */
int
getmessage(StringInfo s, ClientSocket *client_sock , int maxlen)
{
    int32		len;

    Assert(DbCommReadingMsg);

    resetStringInfo(s);

    /* Read message length word */
    if (getbytes((char *) &len, 4, client_sock) == EOF)
    {
        printf("unexpected EOF within message length word");
        return EOF;
    }

    len = ntoh32(len);

    if (len < 4 || len > maxlen)
    {
        printf("invalid message length");
        return EOF;
    }

    len -= 4;					/* discount length itself */

    if (len > 0)
    {
        /*
         * Allocate space for message.  If we run out of room (ridiculously
         * large message), we will elog(ERROR), but we want to discard the
         * message body so as not to lose communication sync.
         */
        //TRY();
        //{
        enlargeStringInfo(s, len);
        //}
        //CATCH();
        //{
        //	if (discardbytes(len) == EOF)
        //				 printf("incomplete message from client");

        /* we discarded the rest of the message so we're back in sync. */
        //	DbCommReadingMsg = false;
        //	RE_THROW();
        //}
        //END_TRY();

        /* And grab the message */
        if (getbytes(s->data, len, client_sock) == EOF)
        {
            printf("incomplete message from client");
            return EOF;
        }
        s->len = len;
        /* Place a trailing null per StringInfo convention */
        s->data[len] = '\0';
    }

    /* finished reading the message. */
    DbCommReadingMsg = false;

    return 0;
}


/* --------------------------------
 *		internal_flush_buffer - flush the given buffer content
 *
 * Returns 0 if OK (meaning everything was sent, or operation would block
 * and the socket is in non-blocking mode), or EOF if trouble.
 * --------------------------------
 */
db_noinline int
internal_flush_buffer(ClientSocket *client_sock , const char *buf, size_t *start, size_t *end)
{
    static int	last_reported_send_errno = 0;

    const char *bufptr = buf + *start;
    const char *bufend = buf + *end;

    while (bufptr < bufend)
    {
        int			r;

        r = be_gssapi_write(client_sock ,(char *) bufptr, bufend - bufptr);

        if (r <= 0)
        {
            if (errno == EINTR)
                continue;		/* Ok if we were interrupted */

            /*
             * Ok if no data writable without blocking, and the socket is in
             * non-blocking mode.
             */
            if (errno == EAGAIN ||
                errno == EWOULDBLOCK)
            {
                return 0;
            }

            /*
             * Careful: an ereport() that tries to write to the client would
             * cause recursion to here, leading to stack overflow and core
             * dump!  This message must go *only* to the postmaster log.
             *
             * If a client disconnects while we're in the midst of output, we
             * might write quite a bit of data before we get to a safe query
             * abort point.  So, suppress duplicate log messages.
             */
            if (errno != last_reported_send_errno)
            {
                last_reported_send_errno = errno;
                printf("could not send data to client: %m");
            }

            /*
             * We drop the buffered data anyway so that processing can
             * continue, even though we'll probably quit soon. We also set a
             * flag that'll cause the next CHECK_FOR_INTERRUPTS to terminate
             * the connection.
             */
            *start = *end = 0;
            ClientConnectionLost = 1;
            InterruptPending = 1;
            return EOF;
        }

        last_reported_send_errno = 0;	/* reset after any successful send */
        bufptr += r;
        *start += r;
    }

    *start = *end = 0;
    return 0;
}

/* --------------------------------
 *	socket_is_send_pending	- is there any pending data in the output buffer?
 * --------------------------------
 */
static bool
socket_is_send_pending(void)
{
    return (DbSendStart < DbSendPointer);
}

/* --------------------------------
 * Message-level I/O routines begin here.
 * --------------------------------
 */



/* --------------------------------
 *		putmessage_noblock	- like putmessage, but never blocks
 *
 *		If the output buffer is too small to hold the message, the buffer
 *		is enlarged.
 */
static void
socket_putmessage_noblock(char msgtype, const char *s, size_t len)
{
    int			res DB_USED_FOR_ASSERTS_ONLY;
    int			required;

    /*
     * Ensure we have enough space in the output buffer for the message header
     * as well as the message itself.
     */
    required = DbSendPointer + 1 + 4 + len;
    if (required > DbSendBufferSize)
    {
        DbSendBuffer = realloc(DbSendBuffer, required);
        DbSendBufferSize = required;
    }
    res = putmessage(msgtype, s, len);
    Assert(res == 0);			/* should not fail when the message fits in
                                 * buffer */
}


/*
 * Support for TCP Keepalive parameters
 */

/*
 * On Windows, we need to set both idle and interval at the same time.
 * We also cannot reset them to the default (setting to zero will
 * actually set them to zero, not default), therefore we fallback to
 * the out-of-the-box default instead.
 */
#if defined(WIN32) && defined(SIO_KEEPALIVE_VALS)
static int
setkeepaliveswin32(Port *port, int idle, int interval)
{
    struct tcp_keepalive ka;
    DWORD		retsize;

    if (idle <= 0)
        idle = 2 * 60 * 60;		/* default = 2 hours */
    if (interval <= 0)
        interval = 1;			/* default = 1 second */

    ka.onoff = 1;
    ka.keepalivetime = idle * 1000;
    ka.keepaliveinterval = interval * 1000;

    if (WSAIoctl(port->sock,
                 SIO_KEEPALIVE_VALS,
                 (LPVOID) &ka,
                 sizeof(ka),
                 NULL,
                 0,
                 &retsize,
                 NULL,
                 NULL)
        != 0)
    {
        fprintf(stderr, "%s(%s) failed: error code %d",
                "WSAIoctl", "SIO_KEEPALIVE_VALS", WSAGetLastError());
        return STATUS_ERROR;
    }
    if (port->keepalives_idle != idle)
        port->keepalives_idle = idle;
    if (port->keepalives_interval != interval)
        port->keepalives_interval = interval;
    return STATUS_OK;
}
#endif

int
getkeepalivesidle(Port *port)
{
#if defined(DB_TCP_KEEPALIVE_IDLE) || defined(SIO_KEEPALIVE_VALS)
    if (port == NULL || port->laddr.addr.ss_family == AF_UNIX)
        return 0;

    if (port->keepalives_idle != 0)
        return port->keepalives_idle;

    if (port->default_keepalives_idle == 0)
    {
#ifndef WIN32
        socklen_t	size = sizeof(port->default_keepalives_idle);

        if (getsockopt(port->sock, IPPROTO_TCP, DB_TCP_KEEPALIVE_IDLE,
                       (char *) &port->default_keepalives_idle,
                       &size) < 0)
        {
            fprintf(stderr, "%s(%s) failed: %m", "getsockopt", DB_TCP_KEEPALIVE_IDLE_STR);
            port->default_keepalives_idle = -1; /* don't know */
        }
#else							/* WIN32 */
        /* We can't get the defaults on Windows, so return "don't know" */
        port->default_keepalives_idle = -1;
#endif							/* WIN32 */
    }

    return port->default_keepalives_idle;
#else
    return 0;
#endif
}

int
setkeepalivesidle(int idle, Port *port)
{
    if (port == NULL || port->laddr.addr.ss_family == AF_UNIX)
        return STATUS_OK;

    /* check SIO_KEEPALIVE_VALS here, not just WIN32, as some toolchains lack it */
#if defined(DB_TCP_KEEPALIVE_IDLE) || defined(SIO_KEEPALIVE_VALS)
    if (idle == port->keepalives_idle)
        return STATUS_OK;

#ifndef WIN32
    if (port->default_keepalives_idle <= 0)
    {
        if (getkeepalivesidle(port) < 0)
        {
            if (idle == 0)
                return STATUS_OK;	/* default is set but unknown */
            else
                return STATUS_ERROR;
        }
    }

    if (idle == 0)
        idle = port->default_keepalives_idle;

    if (setsockopt(port->sock, IPPROTO_TCP, DB_TCP_KEEPALIVE_IDLE,
                   (char *) &idle, sizeof(idle)) < 0)
    {
        fprintf(stderr, "%s(%s) failed: %m", "setsockopt", DB_TCP_KEEPALIVE_IDLE_STR);
        return STATUS_ERROR;
    }

    port->keepalives_idle = idle;
#else							/* WIN32 */
    return setkeepaliveswin32(port, idle, port->keepalives_interval);
#endif
#else
    if (idle != 0)
    {
        fprintf(stderr, "setting the keepalive idle time is not supported");
        return STATUS_ERROR;
    }
#endif

    return STATUS_OK;
}

int
getkeepalivesinterval(Port *port)
{
#if defined(TCP_KEEPINTVL) || defined(SIO_KEEPALIVE_VALS)
    if (port == NULL || port->laddr.addr.ss_family == AF_UNIX)
        return 0;

    if (port->keepalives_interval != 0)
        return port->keepalives_interval;

    if (port->default_keepalives_interval == 0)
    {
#ifndef WIN32
        socklen_t	size = sizeof(port->default_keepalives_interval);

        if (getsockopt(port->sock, IPPROTO_TCP, TCP_KEEPINTVL,
                       (char *) &port->default_keepalives_interval,
                       &size) < 0)
        {
            fprintf(stderr, "%s(%s) failed: %m", "getsockopt", "TCP_KEEPINTVL");
            port->default_keepalives_interval = -1; /* don't know */
        }
#else
        /* We can't get the defaults on Windows, so return "don't know" */
        port->default_keepalives_interval = -1;
#endif							/* WIN32 */
    }

    return port->default_keepalives_interval;
#else
    return 0;
#endif
}

int
setkeepalivesinterval(int interval, Port *port)
{
    if (port == NULL || port->laddr.addr.ss_family == AF_UNIX)
        return STATUS_OK;

#if defined(TCP_KEEPINTVL) || defined(SIO_KEEPALIVE_VALS)
    if (interval == port->keepalives_interval)
        return STATUS_OK;

#ifndef WIN32
    if (port->default_keepalives_interval <= 0)
    {
        if (getkeepalivesinterval(port) < 0)
        {
            if (interval == 0)
                return STATUS_OK;	/* default is set but unknown */
            else
                return STATUS_ERROR;
        }
    }

    if (interval == 0)
        interval = port->default_keepalives_interval;

    if (setsockopt(port->sock, IPPROTO_TCP, TCP_KEEPINTVL,
                   (char *) &interval, sizeof(interval)) < 0)
    {
        fprintf(stderr, "%s(%s) failed: %m", "setsockopt", "TCP_KEEPINTVL");
        return STATUS_ERROR;
    }

    port->keepalives_interval = interval;
#else							/* WIN32 */
    return setkeepaliveswin32(port, port->keepalives_idle, interval);
#endif
#else
    if (interval != 0)
    {
        fprintf(stderr, "%s(%s) not supported", "setsockopt", "TCP_KEEPINTVL");
        return STATUS_ERROR;
    }
#endif

    return STATUS_OK;
}

int
getkeepalivescount(Port *port)
{
#ifdef TCP_KEEPCNT
    if (port == NULL || port->laddr.addr.ss_family == AF_UNIX)
        return 0;

    if (port->keepalives_count != 0)
        return port->keepalives_count;

    if (port->default_keepalives_count == 0)
    {
        socklen_t	size = sizeof(port->default_keepalives_count);

        if (getsockopt(port->sock, IPPROTO_TCP, TCP_KEEPCNT,
                       (char *) &port->default_keepalives_count,
                       &size) < 0)
        {
            fprintf(stderr, "%s(%s) failed: %m", "getsockopt", "TCP_KEEPCNT");
            port->default_keepalives_count = -1;	/* don't know */
        }
    }

    return port->default_keepalives_count;
#else
    return 0;
#endif
}

int
setkeepalivescount(int count, Port *port)
{
    if (port == NULL || port->laddr.addr.ss_family == AF_UNIX)
        return STATUS_OK;

#ifdef TCP_KEEPCNT
    if (count == port->keepalives_count)
        return STATUS_OK;

    if (port->default_keepalives_count <= 0)
    {
        if (getkeepalivescount(port) < 0)
        {
            if (count == 0)
                return STATUS_OK;	/* default is set but unknown */
            else
                return STATUS_ERROR;
        }
    }

    if (count == 0)
        count = port->default_keepalives_count;

    if (setsockopt(port->sock, IPPROTO_TCP, TCP_KEEPCNT,
                   (char *) &count, sizeof(count)) < 0)
    {
        fprintf(stderr, "%s(%s) failed: %m", "setsockopt", "TCP_KEEPCNT");
        return STATUS_ERROR;
    }

    port->keepalives_count = count;
#else
    if (count != 0)
    {
        fprintf(stderr, "%s(%s) not supported", "setsockopt", "TCP_KEEPCNT");
        return STATUS_ERROR;
    }
#endif

    return STATUS_OK;
}

int
gettcpusertimeout(Port *port)
{
#ifdef TCP_USER_TIMEOUT
    if (port == NULL || port->laddr.addr.ss_family == AF_UNIX)
        return 0;

    if (port->tcp_user_timeout != 0)
        return port->tcp_user_timeout;

    if (port->default_tcp_user_timeout == 0)
    {
        socklen_t	size = sizeof(port->default_tcp_user_timeout);

        if (getsockopt(port->sock, IPPROTO_TCP, TCP_USER_TIMEOUT,
                       (char *) &port->default_tcp_user_timeout,
                       &size) < 0)
        {
            fprintf(stderr, "%s(%s) failed: %m", "getsockopt", "TCP_USER_TIMEOUT");
            port->default_tcp_user_timeout = -1;	/* don't know */
        }
    }

    return port->default_tcp_user_timeout;
#else
    return 0;
#endif
}

int
settcpusertimeout(int timeout, Port *port)
{
    if (port == NULL || port->laddr.addr.ss_family == AF_UNIX)
        return STATUS_OK;

#ifdef TCP_USER_TIMEOUT
    if (timeout == port->tcp_user_timeout)
        return STATUS_OK;

    if (port->default_tcp_user_timeout <= 0)
    {
        if (gettcpusertimeout(port) < 0)
        {
            if (timeout == 0)
                return STATUS_OK;	/* default is set but unknown */
            else
                return STATUS_ERROR;
        }
    }

    if (timeout == 0)
        timeout = port->default_tcp_user_timeout;

    if (setsockopt(port->sock, IPPROTO_TCP, TCP_USER_TIMEOUT,
                   (char *) &timeout, sizeof(timeout)) < 0)
    {
        fprintf(stderr, "%s(%s) failed: %m", "setsockopt", "TCP_USER_TIMEOUT");
        return STATUS_ERROR;
    }

    port->tcp_user_timeout = timeout;
#else
    if (timeout != 0)
    {
        fprintf(stderr, "%s(%s) not supported", "setsockopt", "TCP_USER_TIMEOUT");
        return STATUS_ERROR;
    }
#endif

    return STATUS_OK;
}


/*
 * GUC show_hook for tcp_keepalives_idle
 */
const char *
show_tcp_keepalives_idle(void)
{
    /* See comments in assign_tcp_keepalives_idle */
    static char nbuf[16];

    snprintf(nbuf, sizeof(nbuf), "%d", getkeepalivesidle(MyProcPort));
    return nbuf;
}

/*
 * GUC show_hook for tcp_keepalives_interval
 */
const char *
show_tcp_keepalives_interval(void)
{
    /* See comments in assign_tcp_keepalives_idle */
    static char nbuf[16];

    snprintf(nbuf, sizeof(nbuf), "%d", getkeepalivesinterval(MyProcPort));
    return nbuf;
}

/*
 * GUC show_hook for tcp_keepalives_count
 */
const char *
show_tcp_keepalives_count(void)
{
    /* See comments in assign_tcp_keepalives_idle */
    static char nbuf[16];

    snprintf(nbuf, sizeof(nbuf), "%d", getkeepalivescount(MyProcPort));
    return nbuf;
}

/*
 * GUC show_hook for tcp_user_timeout
 */
const char *
show_tcp_user_timeout(void)
{
    /* See comments in assign_tcp_keepalives_idle */
    static char nbuf[16];

    snprintf(nbuf, sizeof(nbuf), "%d", gettcpusertimeout(MyProcPort));
    return nbuf;
}

/*
 * Check if the client is still connected.
 */
bool
check_connection(void)
{
    //WaitEvent	events[FeBeWaitSetNEvents];
    //	int			rc;

    /*
     * It's OK to modify the socket event filter without restoring, because
     * all FeBeWaitSet socket wait sites do the same.
     */
    //ModifyWaitEvent(FeBeWaitSet, FeBeWaitSetSocketPos, WL_SOCKET_CLOSED, NULL);

    //retry:
    //rc = WaitEventSetWait(FeBeWaitSet, 0, events, lengthof(events), 0);
    //for (int i = 0; i < rc; ++i)
    //{
    //	if (events[i].events & WL_SOCKET_CLOSED)
    //		return false;
    //	if (events[i].events & WL_LATCH_SET)
    //	{
    /*
     * A latch event might be preventing other events from being
     * reported.  Reset it and poll again.  No need to restore it
     * because no code should expect latches to survive across
     * CHECK_FOR_INTERRUPTS().
     */
    //			ResetLatch(MyLatch);
    //			goto retry;
    //		}
    //	}

    return true;
}
