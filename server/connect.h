/*-------------------------------------------------------------------------
 *
 * connect.h
 *	  This file contains internal definitions meant to be used only by
 *	  the frontend  library, not by applications that call it.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CONNECT
#define CONNECT


#include <netdb.h>
#include <sys/socket.h>
#include <time.h>
/* MinGW has sys/time.h, but MSVC doesn't */
#ifndef _MSC_VER
#include <sys/time.h>
#endif

#ifdef WIN32
#include "pthread-win32.h"
#else
#include <pthread.h>
#endif
#include <signal.h>
#include <stdio.h>
#include <stdbool.h>
#include <gssapi.h>
#include <gssapi/gssapi.h>

#include "expbuffer.h"

/* Historical names for types in <stdint.h>. */
typedef int8_t int8;
typedef int16_t int16;
typedef int32_t int32;
typedef int64_t int64;
typedef uint8_t uint8;
typedef uint16_t uint16;
typedef uint32_t uint32;
typedef uint64_t uint64;
typedef void (*pgthreadlock_t) (int acquire);


typedef size_t Size;
/* Define a signed 64-bit integer type for use in client API declarations. */
typedef int64_t pg_int64;


/* Application-visible enum types */

/*
 * Although it is okay to add to these lists, values which become unused
 * should never be removed, nor should constants be redefined - that would
 * break compatibility with existing code.
 */

typedef enum
{
    CONNECTION_OK,
    CONNECTION_BAD,
    /* Non-blocking mode only below here */

    /*
     * The existence of these should never be relied upon - they should only
     * be used for user feedback or similar purposes.
     */
    CONNECTION_STARTED,			/* Waiting for connection to be made.  */
    CONNECTION_MADE,			/* Connection OK; waiting to send.     */
    CONNECTION_AWAITING_RESPONSE,	/* Waiting for a response from the
                                     * postmaster.        */
    CONNECTION_AUTH_OK,			/* Received authentication; waiting for
                                 * backend startup. */
    CONNECTION_SETENV,			/* This state is no longer used. */
    CONNECTION_SSL_STARTUP,		/* Performing SSL handshake. */
    CONNECTION_NEEDED,			/* Internal state: connect() needed. */
    CONNECTION_CHECK_WRITABLE,	/* Checking if session is read-write. */
    CONNECTION_CONSUME,			/* Consuming any extra messages. */
    CONNECTION_GSS_STARTUP,		/* Negotiating GSSAPI. */
    CONNECTION_CHECK_TARGET,	/* Internal state: checking target server
                                 * properties. */
    CONNECTION_CHECK_STANDBY,	/* Checking if server is in standby mode. */
    CONNECTION_ALLOCATED,		/* Waiting for connection attempt to be
                                 * started.  */
} ConnStatusType;


typedef enum
{
    PGRES_POLLING_FAILED = 0,
    PGRES_POLLING_READING,          /* These two indicate that one may        */
    PGRES_POLLING_WRITING,          /* use select before polling again.   */
    PGRES_POLLING_OK,
    PGRES_POLLING_ACTIVE            /* unused; keep for backwards compatibility */
} PollingStatusType;


/* Conn encapsulates a connection to the backend.
 * The contents of this struct are not supposed to be known to applications.
 */
typedef struct pg_conn Conn;



/* pg_usec_time_t is like time_t, but with microsecond resolution */
typedef pg_int64 pg_usec_time_t;


/* ----------------
 * Structure for the conninfo parameter definitions returned by conndefaults
 * or conninfoParse.
 *
 * All fields except "val" point at static strings which must not be altered.
 * "val" is either NULL or a malloc'd current-value string.  conninfoFree()
 * will release both the val strings and the conninfoOption array itself.
 * ----------------
 */
typedef struct _conninfoOption
{
    char	   *keyword;		/* The keyword of the option			*/
    char	   *envvar;			/* Fallback environment variable name	*/
    char	   *compiled;		/* Fallback compiled in default value	*/
    char	   *val;			/* Option's current value, or NULL		 */
    char	   *label;			/* Label for field in connect dialog	*/
    char	   *dispchar;		/* Indicates how to display this field in a
                                 * connect dialog. Values are: "" Display
                                 * entered value as is "*" Password field -
                                 * hide value "D"  Debug option - don't show
                                 * by default */
    int			dispsize;		/* Field size in characters for dialog	*/
} conninfoOption;



/* ----------------
 * Exported functions of lib
 * ----------------
 */

/* === in fe-connect.c === */

/* make a new client connection to the backend */
/* Asynchronous (non-blocking) */
extern Conn *connectStart(const char *conninfo);
extern Conn *connectStartParams(const char *const *keywords,
                                const char *const *values);
extern bool connectPoll(Conn *conn);

/* Synchronous (blocking) */
extern Conn *connectdb(const char *conninfo);
extern Conn *connectdbParams(const char *const *keywords,
                             const char *const *values, int expand_dbname);

#define setdb(M_PGHOST,M_PGPORT,M_PGOPT,M_PGTTY,M_DBNAME)  \
    setdbLogin(M_PGHOST, M_PGPORT, M_PGOPT, M_PGTTY, M_DBNAME, NULL, NULL)



/* get info about connection options known to connectdb */
extern conninfoOption *conndefaults(void);

/* parse connection options in same way as connectdb */
extern conninfoOption *conninfoParse(const char *conninfo, char **errmsg);

/* return the connection options used by a live connection */
extern conninfoOption *conninfo(Conn *conn);

/* free the data structure returned by conndefaults() or conninfoParse() */
extern void conninfoFree(conninfoOption *connOptions);

#define LONG_ALIGN_MASK (sizeof(long) - 1)
#define MEMSET_LOOP_LIMIT 1024

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



/*
 * pg_noreturn corresponds to the C11 noreturn/_Noreturn function specifier.
 * We can't use the standard name "noreturn" because some third-party code
 * uses __attribute__((noreturn)) in headers, which would get confused if
 * "noreturn" is defined to "_Noreturn", as is done by <stdnoreturn.h>.
 *
 * In a declaration, function specifiers go before the function name.  The
 * common style is to put them before the return type.  (The MSVC fallback has
 * the same requirement.  The GCC fallback is more flexible.)
 */
#if defined(__STDC_VERSION__) && __STDC_VERSION__ >= 201112L
#define pg_noreturn _Noreturn
#elif defined(__GNUC__) || defined(__SUNPRO_C)
#define pg_noreturn __attribute__((noreturn))
#elif defined(_MSC_VER)
#define pg_noreturn __declspec(noreturn)
#else
#define pg_noreturn
#endif

/* ----------------------------------------------------------------
 *				Section 6:	assertions
 * ----------------------------------------------------------------
 */

/*
 * USE_ASSERT_CHECKING, if defined, turns on all the assertions.
 * - plai  9/5/90
 *
 * It should _NOT_ be defined in releases or in benchmark copies
 */

/*
 * Assert() can be used in both frontend and backend code. In frontend code it
 * just calls the standard assert, if it's available. If use of assertions is
 * not configured, it does nothing.
 */
#ifndef USE_ASSERT_CHECKING

#define Assert(condition)	((void)true)
#define AssertMacro(condition)	((void)true)

#elif defined(FRONTEND)

#include <assert.h>
#define Assert(p) assert(p)
#define AssertMacro(p)	((void) assert(p))

#else							/* USE_ASSERT_CHECKING && !FRONTEND */

/*
 * Assert
 *		Generates a fatal exception if the given condition is false.
 */
#define Assert(condition) \
    do { \
        if (!(condition)) \
        ExceptionalCondition(#condition, __FILE__, __LINE__); \
    } while (0)

/*
 * AssertMacro is the same as Assert but it's suitable for use in
 * expression-like macros, for example:
 *
 *		#define foo(x) (AssertMacro(x != 0), bar(x))
 */
#define AssertMacro(condition) \
    ((void) ((condition) || \
             (ExceptionalCondition(#condition, __FILE__, __LINE__), 0)))

#endif							/* USE_ASSERT_CHECKING && !FRONTEND */

/*
 * Check that `ptr' is `bndr' aligned.
 */
#define AssertPointerAlignment(ptr, bndr) \
    Assert(TYPEALIGN(bndr, (uintptr_t)(ptr)) == (uintptr_t)(ptr))

/*
 * ExceptionalCondition is compiled into the backend whether or not
 * USE_ASSERT_CHECKING is defined, so as to support use of extensions
 * that are built with that #define with a backend that isn't.  Hence,
 * we should declare it as long as !FRONTEND.
 */
#ifndef FRONTEND
pg_noreturn extern void ExceptionalCondition(const char *conditionName,
                                             const char *fileName, int lineNumber);
#endif

/*
 * Macros to support compile-time assertion checks.
 *
 * If the "condition" (a compile-time-constant expression) evaluates to false,
 * throw a compile error using the "errmessage" (a string literal).
 *
 * C11 has _Static_assert(), and most C99 compilers already support that.  For
 * portability, we wrap it into StaticAssertDecl().  _Static_assert() is a
 * "declaration", and so it must be placed where for example a variable
 * declaration would be valid.  As long as we compile with
 * -Wno-declaration-after-statement, that also means it cannot be placed after
 * statements in a function.  Macros StaticAssertStmt() and StaticAssertExpr()
 * make it safe to use as a statement or in an expression, respectively.
 *
 * For compilers without _Static_assert(), we fall back on a kluge that
 * assumes the compiler will complain about a negative width for a struct
 * bit-field.  This will not include a helpful error message, but it beats not
 * getting an error at all.
 */
#ifndef __cplusplus
#ifdef HAVE__STATIC_ASSERT
#define StaticAssertDecl(condition, errmessage) \
    _Static_assert(condition, errmessage)
#define StaticAssertStmt(condition, errmessage) \
    do { _Static_assert(condition, errmessage); } while(0)
#define StaticAssertExpr(condition, errmessage) \
    ((void) ({ StaticAssertStmt(condition, errmessage); true; }))
#else							/* !HAVE__STATIC_ASSERT */
#define StaticAssertDecl(condition, errmessage) \
    extern void static_assert_func(int static_assert_failure[(condition) ? 1 : -1])
#define StaticAssertStmt(condition, errmessage) \
    ((void) sizeof(struct { int static_assert_failure : (condition) ? 1 : -1; }))
#define StaticAssertExpr(condition, errmessage) \
    StaticAssertStmt(condition, errmessage)
#endif							/* HAVE__STATIC_ASSERT */
#else							/* C++ */
#if defined(__cpp_static_assert) && __cpp_static_assert >= 200410
#define StaticAssertDecl(condition, errmessage) \
    static_assert(condition, errmessage)
#define StaticAssertStmt(condition, errmessage) \
    static_assert(condition, errmessage)
#define StaticAssertExpr(condition, errmessage) \
    ({ static_assert(condition, errmessage); })
#else							/* !__cpp_static_assert */
#define StaticAssertDecl(condition, errmessage) \
    extern void static_assert_func(int static_assert_failure[(condition) ? 1 : -1])
#define StaticAssertStmt(condition, errmessage) \
    do { struct static_assert_struct { int static_assert_failure : (condition) ? 1 : -1; }; } while(0)
#define StaticAssertExpr(condition, errmessage) \
    ((void) ({ StaticAssertStmt(condition, errmessage); }))
#endif							/* __cpp_static_assert */
#endif							/* C++ */


/*
 * Compile-time checks that a variable (or expression) has the specified type.
 *
 * AssertVariableIsOfType() can be used as a statement.
 * AssertVariableIsOfTypeMacro() is intended for use in macros, eg
 *		#define foo(x) (AssertVariableIsOfTypeMacro(x, int), bar(x))
 *
 * If we don't have __builtin_types_compatible_p, we can still assert that
 * the types have the same size.  This is far from ideal (especially on 32-bit
 * platforms) but it provides at least some coverage.
 */
#ifdef HAVE__BUILTIN_TYPES_COMPATIBLE_P
#define AssertVariableIsOfType(varname, typename) \
    StaticAssertStmt(__builtin_types_compatible_p(__typeof__(varname), typename), \
                     CppAsString(varname) " does not have type " CppAsString(typename))
#define AssertVariableIsOfTypeMacro(varname, typename) \
    (StaticAssertExpr(__builtin_types_compatible_p(__typeof__(varname), typename), \
                      CppAsString(varname) " does not have type " CppAsString(typename)))
#else							/* !HAVE__BUILTIN_TYPES_COMPATIBLE_P */
#define AssertVariableIsOfType(varname, typename) \
    StaticAssertStmt(sizeof(varname) == sizeof(typename), \
                     CppAsString(varname) " does not have type " CppAsString(typename))
#define AssertVariableIsOfTypeMacro(varname, typename) \
    (StaticAssertExpr(sizeof(varname) == sizeof(typename), \
                      CppAsString(varname) " does not have type " CppAsString(typename)))
#endif							/* HAVE__BUILTIN_TYPES_COMPATIBLE_P */



typedef uint32 ProtocolVersion; /* FE/BE protocol version number */

typedef ProtocolVersion MsgType;
typedef struct
{
    struct sockaddr_storage addr;
    socklen_t	salen;
} SockAddr;

typedef struct
{
    int			family;
    SockAddr	addr;
} AddrInfo;
/* Define to best printf format archetype, usually gnu_printf if available. */
#define PG_PRINTF_ATTRIBUTE gnu_printf

//#define UNIXSOCK_PATH_BUFLEN sizeof(((struct sockaddr_un *) NULL)->sun_path)
/*
 * POSTGRES backend dependent Constants.
 */
#define CMDSTATUS_LEN 64		/* should match COMPLETION_TAG_BUFSIZE */

#define UNIXSOCK_PATH(path, port, sockdir) \
    (AssertMacro(sockdir), \
     AssertMacro(*(sockdir) != '\0'), \
     snprintf(path, sizeof(path), "%s/.s.PGSQL.%d", \
              (sockdir), (port)))



/* Bitmasks for allowed_enc_methods and failed_enc_methods */
#define ENC_ERROR			0
#define ENC_PLAINTEXT		0x01
#define ENC_GSSAPI			0x02
#define ENC_SSL				0x04



/* Boolean value plus a not-known state, for GUCs we might have to fetch */
typedef enum
{
    PG_BOOL_UNKNOWN = 0,		/* Currently unknown */
    PG_BOOL_YES,				/* Yes (true) */
    PG_BOOL_NO					/* No (false) */
} PGTernaryBool;

/* Typedef for the EnvironmentOptions[] array */
typedef struct EnvironmentOption
{
    const char *envName,		/* name of an environment variable */
          *pgName;			/* name of corresponding SET variable */
} EnvironmentOption;



/* PGdataValue represents a data field value being passed to a row processor.
 * It could be either text or binary data; text data is not zero-terminated.
 * A SQL NULL is represented by len < 0; then value is still valid but there
 * are no data bytes there.
 */
typedef struct pgDataValue
{
    int			len;			/* data length in bytes, or <0 if NULL */
    const char *value;			/* data value, without zero-termination */
} PGdataValue;

/* Host address type enum for struct pg_conn_host */
typedef enum pg_conn_host_type
{
    CHT_HOST_NAME,
    CHT_HOST_ADDRESS,
    CHT_UNIX_SOCKET
} pg_conn_host_type;


/*
 * valid values for pg_conn->current_auth_response.  These are just for
 * libpq internal use: since authentication response types all use the
 * protocol byte 'p', fe-trace.c needs a way to distinguish them in order
 * to print them correctly.
 */
#define AUTH_RESPONSE_GSS			'G'
#define AUTH_RESPONSE_PASSWORD		'P'
#define AUTH_RESPONSE_SASL_INITIAL	'I'
#define AUTH_RESPONSE_SASL			'S'

/*
 * An entry in the pending command queue.
 */
typedef struct PGcmdQueueEntry
{
    //PGQueryClass queryclass;	/* Query type */
    char	   *query;			/* SQL command, or NULL if none/unknown/OOM */
    struct PGcmdQueueEntry *next;	/* list link */
} PGcmdQueueEntry;

/*
 * pg_conn_host stores all information about each of possibly several hosts
 * mentioned in the connection string.  Most fields are derived by splitting
 * the relevant connection parameter (e.g., pghost) at commas.
 */
typedef struct pg_conn_host
{
    pg_conn_host_type type;		/* type of host address */
    char	   *host;			/* host name or socket path */
    char	   *hostaddr;		/* host numeric IP address */
    char	   *port;			/* port number (always provided) */
    char	   *password;		/* password for this host, read from the
                                 * password file; NULL if not sought or not
                                 * found in password file. */
} pg_conn_host;

/*
 * Conn stores all the state data associated with a single connection
 * to a backend.
 */
struct pg_conn
{
    /* Saved values of connection options */
    char	   *pghost;			/* the machine on which the server is running,
                                 * or a path to a UNIX-domain socket, or a
                                 * comma-separated list of machines and/or
                                 * paths; if NULL, use DEFAULT_PGSOCKET_DIR */
    char	   *pghostaddr;		/* the numeric IP address of the machine on
                                 * which the server is running, or a
                                 * comma-separated list of same.  Takes
                                 * precedence over pghost. */
    char	   *pgport;			/* the server's communication port number, or
                                 * a comma-separated list of ports */
    char	   *connect_timeout;	/* connection timeout (numeric string) */
    char	   *pgtcp_user_timeout; /* tcp user timeout (numeric string) */
    char	   *client_encoding_initial;	/* encoding to use */
    char	   *pgoptions;		/* options to start the backend with */
    char	   *appname;		/* application name */
    char	   *fbappname;		/* fallback application name */
    char	   *dbName;			/* database name */
    char	   *replication;	/* connect as the replication standby? */
    char	   *pguser;			/* Postgres username and password, if any */
    char	   *pgpass;
    char	   *pgpassfile;		/* path to a file containing password(s) */
    char	   *channel_binding;	/* channel binding mode
                                     * (require,prefer,disable) */
    char	   *keepalives;		/* use TCP keepalives? */
    char	   *keepalives_idle;	/* time between TCP keepalives */
    char	   *keepalives_interval;	/* time between TCP keepalive
                                         * retransmits */
    char	   *keepalives_count;	/* maximum number of TCP keepalive
                                     * retransmits */
    char	   *requirepeer;	/* required peer credentials for local sockets */
    char	   *target_session_attrs;	/* desired session properties */

    /* Optional file to write trace info to */
    FILE	   *Pfdebug;
    int			traceFlags;


    int			nEvents;		/* number of active events */
    int			eventArraySize; /* allocated array size */

    /* Status indicators */
    ConnStatusType status;
    char		last_sqlstate[6];	/* last reported SQLSTATE */
    bool		options_valid;	/* true if OK to attempt connection */
    bool		nonblocking;	/* whether this connection is using nonblock
                                 * sending semantics */
    bool		partialResMode; /* true if single-row or chunked mode */
    bool		singleRowMode;	/* return current query result row-by-row? */
    int			maxChunkSize;	/* return query result in chunks not exceeding
                                 * this number of rows */
    char		copy_is_binary; /* 1 = copy binary, 0 = copy text */
    int			copy_already_done;	/* # bytes already returned in COPY OUT */

    /* Support for multiple hosts in connection string */
    int			nconnhost;		/* # of hosts named in conn string */
    int			whichhost;		/* host we're currently trying/connected to */
    pg_conn_host *connhost;		/* details about each named host */
    char	   *connip;			/* IP address for current network connection */

    /*
     * The pending command queue as a singly-linked list.  Head is the command
     * currently in execution, tail is where new commands are added.
     */
    PGcmdQueueEntry *cmd_queue_head;
    PGcmdQueueEntry *cmd_queue_tail;

    /*
     * To save malloc traffic, we don't free entries right away; instead we
     * save them in this list for possible reuse.
     */
    PGcmdQueueEntry *cmd_queue_recycle;

    /* Connection data */
    int	sock;			/* FD for socket, PGINVALID_SOCKET if
                         * unconnected */
    SockAddr	laddr;			/* Local address */
    SockAddr	raddr;			/* Remote address */
    ProtocolVersion pversion;	/* FE/BE protocol version in use */
    int			sversion;		/* server version, e.g. 70401 for 7.4.1 */
    bool		sigpipe_so;		/* have we masked SIGPIPE via SO_NOSIGPIPE? */
    bool		sigpipe_flag;	/* can we mask SIGPIPE via MSG_NOSIGNAL? */
    bool		write_failed;	/* have we had a write failure on sock? */
    char	   *write_err_msg;	/* write error message, or NULL if OOM */

    bool		auth_required;	/* require an authentication challenge from
                                 * the server? */
    uint32		allowed_auth_methods;	/* bitmask of acceptable AuthRequest
                                         * codes */
    bool		client_finished_auth;	/* have we finished our half of the
                                         * authentication exchange? */
    char		current_auth_response;	/* used by pqTraceOutputMessage to
                                         * know which auth response we're
                                         * sending */

    bool		try_next_addr;	/* time to advance to next address/host? */
    bool		try_next_host;	/* time to advance to next connhost[]? */
    int			naddr;			/* number of addresses returned by getaddrinfo */
    int			whichaddr;		/* the address currently being tried */
    AddrInfo   *addr;			/* the array of addresses for the currently
                                 * tried host */
    bool		send_appname;	/* okay to send application_name? */

    /* Miscellaneous stuff */
    int			be_pid;			/* PID of backend --- needed for cancels */
    int			be_key;			/* key of backend --- needed for cancels */
    int			client_encoding;	/* encoding id */
    bool		std_strings;	/* standard_conforming_strings */
    PGTernaryBool default_transaction_read_only;	/* default_transaction_read_only */
    PGTernaryBool in_hot_standby;	/* in_hot_standby */
    //PGlobjfuncs *lobjfuncs;		/* private state for large-object access fns */
    //pg_prng_state prng_state;	/* prng state for load balancing connections */


    /* Buffer for data received from backend and not yet processed */
    char	   *inBuffer;		/* currently allocated buffer */
    int			inBufSize;		/* allocated size of buffer */
    int			inStart;		/* offset to first unconsumed data in buffer */
    int			inCursor;		/* next byte to tentatively consume */
    int			inEnd;			/* offset to first position after avail data */

    /* Buffer for data not yet sent to backend */
    char	   *outBuffer;		/* currently allocated buffer */
    int			outBufSize;		/* allocated size of buffer */
    int			outCount;		/* number of chars waiting in buffer */

    /* State for constructing messages in outBuffer */
    int			outMsgStart;	/* offset to msg start (length word); if -1,
                                 * msg has no length word */
    int			outMsgEnd;		/* offset to msg end (so far) */

    /* Row processor interface workspace */
    PGdataValue *rowBuf;		/* array for passing values to rowProcessor */
    int			rowBufLen;		/* number of entries allocated in rowBuf */

    /*
     * Status for asynchronous result construction.  If result isn't NULL, it
     * is a result being constructed or ready to return.  If result is NULL
     * and error_result is true, then we need to return a PGRES_FATAL_ERROR
     * result, but haven't yet constructed it; text for the error has been
     * appended to conn->errorMessage.  (Delaying construction simplifies
     * dealing with out-of-memory cases.)  If saved_result isn't NULL, it is a
     * PGresult that will replace "result" after we return that one; we use
     * that in partial-result mode to remember the query's tuple metadata.
     */
    bool		error_result;	/* do we need to make an ERROR result? */



    /*
     * Buffer for current error message.  This is cleared at the start of any
     * connection attempt or query cycle; after that, all code should append
     * messages to it, never overwrite.
     *
     * In some situations we might report an error more than once in a query
     * cycle.  If so, errorMessage accumulates text from all the errors, and
     * errorReported tracks how much we've already reported, so that the
     * individual error PGresult objects don't contain duplicative text.
     */
    ExpBufferData errorMessage;	/* expansible string */
    int			errorReported;	/* # bytes of string already reported */

    /* Buffer for receiving various parts of messages */
    ExpBufferData workBuffer; /* expansible string */


    gss_ctx_id_t gctx;                      /* GSS context */
    gss_name_t      gtarg_nam;              /* GSS target name */

    /* The following are encryption-only */
    bool            gssenc;                 /* GSS encryption is usable */
    gss_cred_id_t gcred;            /* GSS credential temp storage. */

    /* GSS encryption I/O state --- see fe-secure-gssapi.c */
    char       *gss_SendBuffer; /* Encrypted data waiting to be sent */
    int                     gss_SendLength; /* End of data available in gss_SendBuffer */
    int                     gss_SendNext;   /* Next index to send a byte from
                                             * gss_SendBuffer */
    int                     gss_SendConsumed;       /* Number of source bytes encrypted but
                                                     * not yet reported as sent */
    char       *gss_RecvBuffer; /* Received, encrypted data */
    int                     gss_RecvLength; /* End of data available in gss_RecvBuffer */
    char       *gss_ResultBuffer;   /* Decryption of data in gss_RecvBuffer */
    int                     gss_ResultLength;       /* End of data available in
                                                     * gss_ResultBuffer */
    int                     gss_ResultNext; /* Next index to read a byte from
                                             * gss_ResultBuffer */
    uint32          gss_MaxPktSize; /* Maximum size we can encrypt and fit the
                                     * results into our output buffer */
    char       *gssdelegation;      /* Try to delegate GSS credentials? (0 or 1) */
    bool            gssapi_used;    /* true if authenticated via gssapi */
    char       *krbsrvname;         /* Kerberos service name */



};


/* String descriptions of the ExecStatusTypes.
 * direct use of this array is deprecated; call  resStatus() instead.
 */
extern char *const pgresStatus[];


/* ----------------
 * Internal functions of lib
 * Functions declared here need to be visible across files of lib ,
 * but are not intended to be called by applications.  We use the
 * convention " XXX" for internal functions, vs. the " xxx" names
 * used for application-visible routines.
 * ----------------
 */

/* === in fe-connect.c === */

extern void  DropConnection(Conn *conn, bool flushInput);
extern bool  ConnectOptions2(Conn *conn);
#if defined(WIN32) && defined(SIO_KEEPALIVE_VALS)
extern int	 SetKeepalivesWin32(pgsocket sock, int idle, int interval);
#endif
extern int	 ConnectStart(Conn *conn);
extern int	 ConnectDBComplete(Conn *conn);
extern Conn * MakeEmptyConn(void);
extern void  ReleaseConnHosts(Conn *conn);
extern void  CloseConn(Conn *conn);
extern int	 PacketSend(Conn *conn, char pack_type,
                        const void *buf, size_t buf_len);
extern bool  GetHomeDirectory(char *buf, int bufsize);
extern bool  CopyConn(Conn *srcConn, Conn *dstConn);
extern bool  ParseIntParam(const char *value, int *result, Conn *conn,
                           const char *context);

extern pgthreadlock_t pg_g_threadlock;

#define pglock_thread()		pg_g_threadlock(true)
#define pgunlock_thread()	pg_g_threadlock(false)

/* === in fe-exec.c === */

extern void  ClearAsyncResult(Conn *conn);
extern void  SaveErrorResult(Conn *conn);
extern void  SaveParameterStatus(Conn *conn, const char *name,
                                 const char *value);
extern int	 RowProcessor(Conn *conn, const char **errmsgp);
extern void  CommandQueueAdvance(Conn *conn, bool isReadyForQuery,
                                 bool gotSync);
extern int	 sendQueryContinue(Conn *conn, const char *query);

/* === in fe-protocol3.c === */

extern char * BuildStartupPacket3(Conn *conn, int *packetlen,
                                  const  EnvironmentOption *options);
extern void  ParseInput3(Conn *conn);
extern int	 GetErrorNotice3(Conn *conn, bool isError);
extern int	 GetNegotiateProtocolVersion3(Conn *conn);
extern int	 GetCopyData3(Conn *conn, char **buffer, int async);
extern int	 Getline3(Conn *conn, char *s, int maxlen);
extern int	 GetlineAsync3(Conn *conn, char *buffer, int bufsize);
extern int	 Endcopy3(Conn *conn);

/* === in fe-misc.c === */

/*
 * "Get" and "Put" routines return 0 if successful, EOF if not. Note that for
 * Get, EOF merely means the buffer is exhausted, not that there is
 * necessarily any error.
 */
extern int	 CheckOutBufferSpace(size_t bytes_needed, Conn *conn);
extern int	 CheckInBufferSpace(size_t bytes_needed, Conn *conn);
extern void  ParseDone(Conn *conn, int newInStart);
extern int	 Getc(char *result, Conn *conn);
extern int	 Putc(char c, Conn *conn);
extern int	 Gets( ExpBuffer buf, Conn *conn);
extern int	 Gets_append( ExpBuffer buf, Conn *conn);
extern int	 Puts(const char *s, Conn *conn);
extern int	 Getnchar(char *s, size_t len, Conn *conn);
extern int	 Skipnchar(size_t len, Conn *conn);
extern int	 Putnchar(const char *s, size_t len, Conn *conn);
extern int	 GetInt(int *result, size_t bytes, Conn *conn);
extern int	 PutInt(int value, size_t bytes, Conn *conn);
extern int	 PutMsgStart(char msg_type, Conn *conn);
extern int	 PutMsgEnd(Conn *conn);
extern int	 ReadData(Conn *conn);
extern int	 Flush(Conn *conn);
extern int	 Wait(int forRead, int forWrite, Conn *conn);
extern int	 WaitTimed(int forRead, int forWrite, Conn *conn,
                       pg_usec_time_t end_time);
extern int	 ReadReady(Conn *conn);
extern int	 WriteReady(Conn *conn);
/* Poll a socket for reading and/or writing with an optional timeout */
extern int      socketPoll(int sock, int forRead, int forWrite,
                           pg_usec_time_t end_time);
extern ssize_t pg_GSS_write(Conn *conn, const void *ptr, size_t len);
extern ssize_t pg_GSS_read(Conn *conn, void *ptr, size_t len);
void
pg_GSS_error(const char *mprefix, Conn *conn,
             OM_uint32 maj_stat, OM_uint32 min_stat);
int
pg_GSS_load_servicename(Conn *conn);
bool
pg_GSS_have_cred_cache(gss_cred_id_t *cred_out);
ssize_t
secure_raw_write(Conn *conn, const void *ptr, size_t len);
ssize_t
secure_raw_read(Conn *conn, void *ptr, size_t len);
PollingStatusType
pqsecure_open_gss(Conn *conn);


/* === miscellaneous macros === */

/*
 * Reset the conn's error-reporting state.
 */
#define  ClearConnErrorState(conn) \
    (reset ExpBuffer(&(conn)->errorMessage), \
     (conn)->errorReported = 0)

/*
 * Check whether we have a PGresult pending to be returned --- either a
 * constructed one in conn->result, or a "virtual" error result that we
 * don't intend to materialize until the end of the query cycle.
 */
#define pgHavePendingResult(conn) \
    ((conn)->result != NULL || (conn)->error_result)

/*
 * this is so that we can check if a connection is non-blocking internally
 * without the overhead of a function call
 */
#define  Isnonblocking(conn)	((conn)->nonblocking)

/*
 * Connection's outbuffer threshold, for pipeline mode.
 */
#define OUTBUFFER_THRESHOLD	65536

#ifdef ENABLE_NLS
extern char *db_gettext(const char *msgid) db_attribute_format_arg(1);
extern char *db_ngettext(const char *msgid, const char *msgid_plural, unsigned long n) pg_attribute_format_arg(1) pg_attribute_format_arg(2);
#else
#define libpq_gettext(x) (x)
#define libpq_ngettext(s, p, n) ((n) == 1 ? (s) : (p))
#endif
/*
 * libpq code should use the above, not _(), since that would use the
 * surrounding programs's message catalog.
 */
#undef _

extern void db_append_error( ExpBuffer errorMessage, const char *fmt,...) db_attribute_printf(2, 3);
extern void db_append_conn_error(Conn *conn, const char *fmt,...) db_attribute_printf(2, 3);

/*
 * These macros are needed to let error-handling code be portable between
 * Unix and Windows.  (ugh)
 */
#ifdef WIN32
#define SOCK_ERRNO (WSAGetLastError())
#define SOCK_STRERROR winsock_strerror
#define SOCK_ERRNO_SET(e) WSASetLastError(e)
#else
#define SOCK_ERRNO errno
#define SOCK_STRERROR strerror_r
#define SOCK_ERRNO_SET(e) (errno = (e))
#endif

#endif							/* LIBPQ_INT_H */
