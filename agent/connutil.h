/*-------------------------------------------------------------------------
 *
 * connutil.h
 *-------------------------------------------------------------------------
 */
#ifndef ONNUTIL_H
#define ONNUTIL_H

#include <netinet/in.h>
#include "comm.h"
#include "stringinfo.h"
#include <stdbool.h>


#define DBDLLIMPORT

/*
 * ClientConnectionInfo includes the fields describing the client connection
 * that are copied over to parallel workers as nothing from Port does that.
 * The same rules apply for allocations here as for Port (everything must be
 * malloc'd ).
 *
 * If you add a struct member here, remember to also handle serialization in
 * SerializeClientConnectionInfo() and co.
 */
typedef struct ClientConnectionInfo
{

    const char *authn_id;

    /*
     * The HBA method that determined the above authn_id.  This only has
     * meaning if authn_id is not NULL; otherwise it's undefined.
     */
    //UserAuth	auth_method;
} ClientConnectionInfo;

/*
 * The Port structure holds state information about a client connection in a
 * backend process.  It is available in the global variable MyProcPort.  The
 * struct and all the data it points are kept in memory.
 *
 * remote_hostname is set if we did a successful reverse lookup of the
 * client's IP address during connection setup.
 * remote_hostname_resolv tracks the state of hostname verification:
 *	+1 = remote_hostname is known to resolve to client's IP address
 *	-1 = remote_hostname is known NOT to resolve to client's IP address
 *	 0 = we have not done the forward DNS lookup yet
 *	-2 = there was an error in name resolution
 * If reverse lookup of the client IP address fails, remote_hostname will be
 * left NULL while remote_hostname_resolv is set to -2.  If reverse lookup
 * succeeds but forward lookup fails, remote_hostname_resolv is also set to -2
 * (the case is distinguishable because remote_hostname isn't NULL).  In
 * either of the -2 cases, remote_hostname_errcode saves the lookup return
 * code for possible later use with gai_strerror.
 */

typedef struct Port
{
    int	sock;			/* File descriptor */
    bool		noblock;		/* is the socket in non-blocking mode? */
    SockAddr	laddr;			/* local addr (postmaster) */
    SockAddr	raddr;			/* remote addr (client) */
    char	   *remote_host;	/* name (or ip addr) of remote host */
    char	   *remote_hostname;	/* name (not ip addr) of remote host, if
                                     * available */
    int			remote_hostname_resolv; /* see above */
    int			remote_hostname_errcode;	/* see above */
    char	   *remote_port;	/* text rep of remote port */

    /*
     * Information that needs to be saved from the startup packet and passed
     * into backend execution.  "char *" fields are NULL if not set.
     * guc_options points to a List of alternating option names and values.
     */
    char	   *database_name;
    char	   *user_name;
    char	   *cmdline_options;

    /*
     * The startup packet application name, only used here for the "connection
     * authorized" log message. We shouldn't use this post-startup, instead
     * the GUC should be used as application can change it afterward.
     */
    char	   *application_name;

    /*
     * TCP keepalive and user timeout settings.
     *
     * default values are 0 if AF_UNIX or not yet known; current values are 0
     * if AF_UNIX or using the default. Also, -1 in a default value means we
     * were unable to find out the default (getsockopt failed).
     */
    int			default_keepalives_idle;
    int			default_keepalives_interval;
    int			default_keepalives_count;
    int			default_tcp_user_timeout;
    int			keepalives_idle;
    int			keepalives_interval;
    int			keepalives_count;
    int			tcp_user_timeout;

    /*
     * SSL structures.
     */
    bool		ssl_in_use;
    char	   *peer_cn;
    char	   *peer_dn;
    bool		peer_cert_valid;
    bool		alpn_used;
    bool		last_read_was_eof;


    /*
     * This is a bit of a hack. raw_buf is data that was previously read and
     * buffered in a higher layer but then "unread" and needs to be read again
     * while establishing an SSL connection via the SSL library layer.
     *
     * There's no API to "unread", the upper layer just places the data in the
     * Port structure in raw_buf and sets raw_buf_remaining to the amount of
     * bytes unread and raw_buf_consumed to 0.
     */
    char	   *raw_buf;
    ssize_t		raw_buf_consumed,
                raw_buf_remaining;
} Port;

/*
 * ClientSocket holds a socket for an accepted connection, along with the
 * information about the remote endpoint.  This is passed from postmaster to
 * the backend process.
 */
typedef struct ClientSocket
{
    int	sock;			/* File descriptor */
    SockAddr	raddr;			/* remote addr (client) */
} ClientSocket;

extern ClientSocket *global_client_socket;

extern DBDLLIMPORT ClientConnectionInfo MyClientConnectionInfo;

/* TCP keepalives configuration. These are no-ops on an AF_UNIX socket. */

extern int	getkeepalivesidle(Port *port);
extern int	getkeepalivesinterval(Port *port);
extern int	getkeepalivescount(Port *port);
extern int	gettcpusertimeout(Port *port);

extern int	setkeepalivesidle(int idle, Port *port);
extern int	setkeepalivesinterval(int interval, Port *port);
extern int	setkeepalivescount(int count, Port *port);
extern int	settcpusertimeout(int timeout, Port *port);

/*
 * Callers of getmessage() must supply a maximum expected message size.
 * By convention, if there's not any specific reason to use another value,
 * use DB_SMALL_MESSAGE_LIMIT for messages that shouldn't be too long, and
 * DB_LARGE_MESSAGE_LIMIT for messages that can be long.
 */
#define DB_SMALL_MESSAGE_LIMIT	10000
#define DB_LARGE_MESSAGE_LIMIT	(MaxAllocSize - 1)

typedef struct
{
    void		(*comm_reset) (void);
    int			(*flush) (void);
    int			(*flush_if_writable) (void);
    bool		(*is_send_pending) (void);
    int			(*putmessage) (char msgtype, const char *s, size_t len);
    void		(*putmessage_noblock) (char msgtype, const char *s, size_t len);
} DBcommMethods;

extern const DBDLLIMPORT DBcommMethods *DbCommMethods;

#define db_comm_reset() (DbCommMethods->comm_reset())
#define flush() (DbCommMethods->flush())
#define flush_if_writable() (DbCommMethods->flush_if_writable())
#define is_send_pending() (DbCommMethods->is_send_pending())
#define putmessage(msgtype, s, len) \
    (DbCommMethods->putmessage(msgtype, s, len))
#define putmessage_noblock(msgtype, s, len) \
    (DbCommMethods->putmessage_noblock(msgtype, s, len))

/*
 * External functions.
 */

/*
 * prototypes for functions in pqcomm.c
 */
//extern DBDLLIMPORT WaitEventSet *FeBeWaitSet;

#define FeBeWaitSetSocketPos 0
#define FeBeWaitSetLatchPos 1
#define FeBeWaitSetNEvents 3

/*
 * Forcing a function not to be inlined can be useful if it's the slow path of
 * a performance-critical function, or should be visible in profiles to allow
 * for proper cost attribution.  Note that unlike the pg_attribute_XXX macros
 * above, this should be placed before the function's return type and name.
 */
/* GCC and Sunpro support noinline via __attribute__ */
#if (defined(__GNUC__) && __GNUC__ > 2) || defined(__SUNPRO_C)
#define db_noinline __attribute__((noinline))
/* msvc via declspec */
#elif defined(_MSC_VER)
#define db_noinline __declspec(noinline)
#else
#define db_noinline
#endif


extern int	ListenServerPort(int family, const char *hostName,
                             unsigned short portNumber,
                             int ListenSockets[], int *NumListenSockets, int MaxListen);
extern int	AcceptConnection(int server_fd, ClientSocket *client_sock);
extern void TouchSocketFiles(void);
extern void RemoveSocketFiles(void);
extern Port *init(ClientSocket *client_sock);
extern int	getbytes(char *s, size_t len, ClientSocket *client_sock);
extern void startmsgread(void);
extern void endmsgread(void);
extern bool is_reading_msg(void);
extern int	getmessage(StringInfo s, ClientSocket *client_sock ,int maxlen);
extern int	getbyte(ClientSocket *client_sock);
extern int	peekbyte(void);
extern int	getbyte_if_available(unsigned char *c);
extern ssize_t buffer_remaining_data(void);
extern int	putmessage_v2(char msgtype, const char *s, size_t len);
extern bool check_connection(void);
extern db_noinline int
internal_flush_buffer(ClientSocket *client_sock , const char *buf, size_t *start, size_t *end);
/*
 * prototypes for functions in be-secure.c
 */
extern int	secure_initialize(bool isServerStart);
extern bool secure_loaded_verify_locations(void);
extern void secure_destroy(void);
extern void secure_close(Port *port);
extern ssize_t secure_read(Port *port, void *ptr, size_t len);
extern ssize_t secure_write(Port *port, void *ptr, size_t len);
extern ssize_t secure_raw_read(ClientSocket *client_sock, void *ptr, size_t len);
extern ssize_t secure_raw_write(ClientSocket *client_sock, const void *ptr, size_t len);

ssize_t
be_gssapi_write(ClientSocket *client_sock, void *ptr, size_t len);
ssize_t
be_gssapi_read(ClientSocket *client_sock, void *ptr, size_t len);
void
pg_GSS_error(const char *errmsg,
             OM_uint32 maj_stat, OM_uint32 min_stat);
void
pg_store_delegated_credential(gss_cred_id_t cred);

ssize_t secure_open_gssapi(ClientSocket *client_sock);
#endif
