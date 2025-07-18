/*-------------------------------------------------------------------------
 *
 * connect.c
 *	  functions related to setting up a connection to the backend
 */

#define _GNU_SOURCE
#include <sys/stat.h>
#include <fcntl.h>
#include <ctype.h>
#include <netdb.h>
#include <time.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <stdlib.h>
#include <string.h>

#include "connect.h"

#ifdef WIN32
#include "win32.h"
#ifdef _WIN32_IE
#undef _WIN32_IE
#endif
#define _WIN32_IE 0x0500
#ifdef near
#undef near
#endif
#define near
#include <shlobj.h>
#include <mstcpip.h>
#else
#include <sys/socket.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pwd.h>
#endif

#include <pthread.h>
#include <stddef.h>
#include <errno.h>

#define PGPASSFILE "pgpass.conf"

#define PG_STRERROR_R_BUFLEN 256        /* Recommended buffer size for strerror_r */

/*
 * fall back options if they are not specified by arguments or defined
 * by environment variables
 */
#define DefaultHost		"localhost"
#define DefaultOption	""
#define DefaultTargetSessionAttrs	"any"
#define MAXPGPATH               1024
#define DEF_PGPORT 1221
#define PGINVALID_SOCKET (-1)
#define DEFAULT_PGSOCKET_DIR "/tmp"
#define DEF_PGPORT_STR "1221"

typedef struct _internalconninfoOption
{
	char	   *keyword;		/* The keyword of the option			*/
	char	   *envvar;			/* Fallback environment variable name	*/
	char	   *compiled;		/* Fallback compiled in default value	*/
	char	   *val;			/* Option's current value, or NULL		*/
	char	   *label;			/* Label for field in connect dialog	*/
	char	   *dispchar;		/* Indicates how to display this field in a
								 * connect dialog. Values are: "" Display
								 * entered value as is "*" Password field -
								 * hide value "D"  Debug option - don't show
								 * by default */
	int			dispsize;		/* Field size in characters for dialog	*/
	/* ---
	 * Anything above this comment must be synchronized with
	 * conninfoOption in libpq-fe.h, since we memcpy() data
	 * between them!
	 * ---
	 */
	off_t		connofs;		/* Offset into Conn struct, -1 if not there */
} internalconninfoOption;

static const internalconninfoOption conninfoOptions[] = {
	{"user", "PGUSER", NULL, NULL,
		"Database-User", "", 20,
	offsetof(struct pg_conn, pguser)},

	{"password", "PGPASSWORD", NULL, NULL,
		"Database-Password", "*", 20,
	offsetof(struct pg_conn, pgpass)},

	{"passfile", "PGPASSFILE", NULL, NULL,
		"Database-Password-File", "", 64,
	offsetof(struct pg_conn, pgpassfile)},
	{"connect_timeout", "ConnECT_TIMEOUT", NULL, NULL,
		"Connect-timeout", "", 10,	/* strlen(INT32_MAX) == 10 */
	offsetof(struct pg_conn, connect_timeout)},

	{"host", "PGHOST", NULL, NULL,
		"Database-Host", "", 40,
	offsetof(struct pg_conn, pghost)},

	{"hostaddr", "PGHOSTADDR", NULL, NULL,
		"Database-Host-IP-Address", "", 45,
	offsetof(struct pg_conn, pghostaddr)},

	{"port", "PGPORT", DEF_PGPORT_STR, NULL,
		"Database-Port", "", 6,
	offsetof(struct pg_conn, pgport)},

	{"client_encoding", "PGCLIENTENCODING", NULL, NULL,
		"Client-Encoding", "", 10,
	offsetof(struct pg_conn, client_encoding_initial)},

	{"options", "PGOPTIONS", DefaultOption, NULL,
		"Backend-Options", "", 40,
	offsetof(struct pg_conn, pgoptions)},

	{"tcp_user_timeout", NULL, NULL, NULL,
		"TCP-User-Timeout", "", 10, /* strlen(INT32_MAX) == 10 */
	offsetof(struct pg_conn, pgtcp_user_timeout)},


	/* Terminating entry --- MUST BE LAST */
	{NULL, NULL, NULL, NULL,
	NULL, NULL, 0, -1}
};


/* The connection URI must start with either of the following designators: */
static const char uri_designator[] = "debo://";
static const char short_uri_designator[] = "debo://";

static bool fillConn(Conn *conn, conninfoOption *connOptions);
static int	store_conn_addrinfo(Conn *conn, struct addrinfo *addrlist);
static conninfoOption *conninfo_init(ExpBuffer errorMessage);
static conninfoOption *parse_connection_string(const char *connstr,
												 ExpBuffer errorMessage, bool use_defaults);
static int	uri_prefix_length(const char *connstr);
static conninfoOption *conninfo_parse(const char *conninfo,
										ExpBuffer errorMessage, bool use_defaults);
static conninfoOption *conninfo_array_parse(const char *const *keywords,
											  const char *const *values, ExpBuffer errorMessage,
											  bool use_defaults);
static bool conninfo_add_defaults(conninfoOption *options,
								  ExpBuffer errorMessage);
static conninfoOption *conninfo_uri_parse(const char *uri,
											ExpBuffer errorMessage, bool use_defaults);
static bool conninfo_uri_parse_options(conninfoOption *options,
									   const char *uri, ExpBuffer errorMessage);
static bool conninfo_uri_parse_params(char *params,
									  conninfoOption *connOptions,
									  ExpBuffer errorMessage);
static char *conninfo_uri_decode(const char *str, ExpBuffer errorMessage);
static bool get_hexdigit(char digit, int *value);
static const char *conninfo_getval(conninfoOption *connOptions,
								   const char *keyword);
static conninfoOption *conninfo_storeval(conninfoOption *connOptions,
										   const char *keyword, const char *value,
										   ExpBuffer errorMessage, bool ignoreMissing, bool uri_decode);
static conninfoOption *conninfo_find(conninfoOption *connOptions,
									   const char *keyword);
/*
 *		connectStartParams
 *
 * Begins the establishment of a connection to a backend through the
 * using connection information in a struct.
 *
 * Returns a Conn*.  If NULL is returned, a malloc error has occurred, and
 * you should not attempt to proceed with this connection.  If the status
 * field of the connection returned is CONNECTION_BAD, an error has
 * occurred. In this case you should call finish on the result, (perhaps
 * inspecting the error message first).  Other fields of the structure may not
 * be valid if that occurs.  If the status field is not CONNECTION_BAD, then
 * this stage has succeeded - call connectPoll, using select(2) to see when
 * this is necessary.
 *
 * See connectPoll for more info.
 */
Conn *
connectStartParams(const char *const *keywords,
					 const char *const *values)
{
	Conn	   *conn;
	conninfoOption *connOptions;

	/*
	 * Allocate memory for the conn structure.  Note that we also expect this
	 * to initialize conn->errorMessage to empty.  All subsequent steps during
	 * connection initialization will only append to that buffer.
	 */
	conn = MakeEmptyConn();
	if (conn == NULL)
		return NULL;

	/*
	 * Parse the conninfo arrays
	 */
	connOptions = conninfo_array_parse(keywords, values,
									   &conn->errorMessage,
									   true);
	if (connOptions == NULL)
	{
		conn->status = CONNECTION_BAD;
		/* errorMessage is already set */
		return conn;
	}

	/*
	 * Move option values into conn structure
	 */
	if (!fillConn(conn, connOptions))
	{
		return conn;
	}

	/*
	 * Compute derived options
	 */
	if (!ConnectOptions2(conn))
		return conn;

	/*
	 * Connect to the database
	 */
	if (!ConnectStart(conn))
	{
		/* Just in case we failed to set it in ConnectDBStart */
		conn->status = CONNECTION_BAD;
	}

	return conn;
}



/*
 * Move option values into conn structure
 *
 * Don't put anything cute here --- intelligence should be in
 * ConnectOptions2 ...
 *
 * Returns true on success. On failure, returns false and sets error message.
 */
static bool
fillConn(Conn *conn, conninfoOption *connOptions)
{
	const internalconninfoOption *option;

	for (option = conninfoOptions; option->keyword; option++)
	{
		if (option->connofs >= 0)
		{
			const char *tmp = conninfo_getval(connOptions, option->keyword);

			if (tmp)
			{
				char	  **connmember = (char **) ((char *) conn + option->connofs);

				free(*connmember);
				*connmember = strdup(tmp);
				if (*connmember == NULL)
				{
					db_append_conn_error(conn, "out of memory");
					return false;
				}
			}
		}
	}

	return true;
}


/*
 * Count the number of elements in a simple comma-separated list.
 */
static int
count_comma_separated_elems(const char *input)
{
	int			n;

	n = 1;
	for (; *input != '\0'; input++)
	{
		if (*input == ',')
			n++;
	}

	return n;
}

/*
 * Parse a simple comma-separated list.
 *
 * On each call, returns a malloc'd copy of the next element, and sets *more
 * to indicate whether there are any more elements in the list after this,
 * and updates *startptr to point to the next element, if any.
 *
 * On out of memory, returns NULL.
 */
static char *
parse_comma_separated_list(char **startptr, bool *more)
{
	char	   *p;
	char	   *s = *startptr;
	char	   *e;
	int			len;

	/*
	 * Search for the end of the current element; a comma or end-of-string
	 * acts as a terminator.
	 */
	e = s;
	while (*e != '\0' && *e != ',')
		++e;
	*more = (*e == ',');

	len = e - s;
	p = (char *) malloc(sizeof(char) * (len + 1));
	if (p)
	{
		memcpy(p, s, len);
		p[len] = '\0';
	}
	*startptr = e + 1;

	return p;
}



/*
 *		ConnectOptions2
 *
 * Compute derived connection options after absorbing all user-supplied info.
 *
 * Returns true if OK, false if trouble (in which case errorMessage is set
 * and so is conn->status).
 */
bool
ConnectOptions2(Conn *conn)
{
	int			i;

	/*
	 * Allocate memory for details about each host to which we might possibly
	 * try to connect.  For that, count the number of elements in the hostaddr
	 * or host options.  If neither is given, assume one host.
	 */
	conn->whichhost = 0;
	if (conn->pghostaddr && conn->pghostaddr[0] != '\0')
		conn->nconnhost = count_comma_separated_elems(conn->pghostaddr);
	else if (conn->pghost && conn->pghost[0] != '\0')
		conn->nconnhost = count_comma_separated_elems(conn->pghost);
	else
		conn->nconnhost = 1;
	conn->connhost = (pg_conn_host *)
		calloc(conn->nconnhost, sizeof(pg_conn_host));
	if (conn->connhost == NULL)
		goto oom_error;

	/*
	 * We now have one pg_conn_host structure per possible host.  Fill in the
	 * host and hostaddr fields for each, by splitting the parameter strings.
	 */
	if (conn->pghostaddr != NULL && conn->pghostaddr[0] != '\0')
	{
		char	   *s = conn->pghostaddr;
		bool		more = true;

		for (i = 0; i < conn->nconnhost && more; i++)
		{
			conn->connhost[i].hostaddr = parse_comma_separated_list(&s, &more);
			if (conn->connhost[i].hostaddr == NULL)
				goto oom_error;
		}

		/*
		 * If hostaddr was given, the array was allocated according to the
		 * number of elements in the hostaddr list, so it really should be the
		 * right size.
		 */
		Assert(!more);
		Assert(i == conn->nconnhost);
	}

	if (conn->pghost != NULL && conn->pghost[0] != '\0')
	{
		char	   *s = conn->pghost;
		bool		more = true;

		for (i = 0; i < conn->nconnhost && more; i++)
		{
			conn->connhost[i].host = parse_comma_separated_list(&s, &more);
			if (conn->connhost[i].host == NULL)
				goto oom_error;
		}

		/* Check for wrong number of host items. */
		if (more || i != conn->nconnhost)
		{
			conn->status = CONNECTION_BAD;
			db_append_conn_error(conn, "could not match %d host names to %d hostaddr values",
									count_comma_separated_elems(conn->pghost), conn->nconnhost);
			return false;
		}
	}

	/*
	 * Now, for each host slot, identify the type of address spec, and fill in
	 * the default address if nothing was given.
	 */
	for (i = 0; i < conn->nconnhost; i++)
	{
		pg_conn_host *ch = &conn->connhost[i];

		if (ch->hostaddr != NULL && ch->hostaddr[0] != '\0')
			ch->type = CHT_HOST_ADDRESS;
		else if (ch->host != NULL && ch->host[0] != '\0')
		{
			ch->type = CHT_HOST_NAME;
		}
		else
		{
			free(ch->host);

			/*
			 * This bit selects the default host location.  If you change
			 * this, see also pg_regress.
			 */
			if (DEFAULT_PGSOCKET_DIR[0])
			{
				ch->host = strdup(DEFAULT_PGSOCKET_DIR);
				ch->type = CHT_UNIX_SOCKET;
			}
			else
			{
				ch->host = strdup(DefaultHost);
				ch->type = CHT_HOST_NAME;
			}
			if (ch->host == NULL)
				goto oom_error;
		}
	}

	/*
	 * Next, work out the port number corresponding to each host name.
	 *
	 * Note: unlike the above for host names, this could leave the port fields
	 * as null or empty strings.  We will substitute DEF_PGPORT whenever we
	 * read such a port field.
	 */
	if (conn->pgport != NULL && conn->pgport[0] != '\0')
	{
		char	   *s = conn->pgport;
		bool		more = true;

		for (i = 0; i < conn->nconnhost && more; i++)
		{
			conn->connhost[i].port = parse_comma_separated_list(&s, &more);
			if (conn->connhost[i].port == NULL)
				goto oom_error;
		}

		/*
		 * If exactly one port was given, use it for every host.  Otherwise,
		 * there must be exactly as many ports as there were hosts.
		 */
		if (i == 1 && !more)
		{
			for (i = 1; i < conn->nconnhost; i++)
			{
				conn->connhost[i].port = strdup(conn->connhost[0].port);
				if (conn->connhost[i].port == NULL)
					goto oom_error;
			}
		}
		else if (more || i != conn->nconnhost)
		{
			conn->status = CONNECTION_BAD;
			db_append_conn_error(conn, "could not match %d port numbers to %d hosts",
									count_comma_separated_elems(conn->pgport), conn->nconnhost);
			return false;
		}
	}


	/*
	 * If password was not given, try to look it up in password file.  Note
	 * that the result might be different for each host/port pair.
	 */
	if (conn->pgpass == NULL || conn->pgpass[0] == '\0')
	{
		/* If password file wasn't specified, use ~/PGPASSFILE */
		if (conn->pgpassfile == NULL || conn->pgpassfile[0] == '\0')
		{
			char		homedir[MAXPGPATH];

			if (GetHomeDirectory(homedir, sizeof(homedir)))
			{
				free(conn->pgpassfile);
				conn->pgpassfile = malloc(MAXPGPATH);
				if (!conn->pgpassfile)
					goto oom_error;
				int ret = snprintf(conn->pgpassfile, MAXPGPATH, "%s/%s",
						 homedir, PGPASSFILE);
						 if (ret >= MAXPGPATH) {
						 db_append_conn_error(conn, "size error");
						  }

			}
		}
	}

	/*
	 * Only if we get this far is it appropriate to try to connect. (We need a
	 * state flag, rather than just the boolean result of this function, in
	 * case someone tries to reset() the Conn.)
	 */
	conn->options_valid = true;

	return true;

oom_error:
	conn->status = CONNECTION_BAD;
	db_append_conn_error(conn, "out of memory");
	return false;
}

/*
 *		conndefaults
 *
 * Construct a default connection options array, which identifies all the
 * available options and shows any default values that are available from the
 * environment etc.  On error (eg out of memory), NULL is returned.
 *
 * Using this function, an application may determine all possible options
 * and their current default values.
 */
conninfoOption *
conndefaults(void)
{
	ExpBufferData errorBuf;
	conninfoOption *connOptions;

	/* We don't actually report any errors here, but callees want a buffer */
	initExpBuffer(&errorBuf);
	if (ExpBufferDataBroken(errorBuf))
		return NULL;			/* out of memory already :-( */

	connOptions = conninfo_init(&errorBuf);
	if (connOptions != NULL)
	{
		/* pass NULL errorBuf to ignore errors */
		if (!conninfo_add_defaults(connOptions, NULL))
		{
			connOptions = NULL;
		}
	}

	termExpBuffer(&errorBuf);
	return connOptions;
}


/* ----------
 * connectNoDelay -
 * Sets the TCP_NODELAY socket option.
 * Returns 1 if successful, 0 if not.
 * ----------
 */
static int
connectNoDelay(Conn *conn)
{
#ifdef	TCP_NODELAY
	int			on = 1;

	if (setsockopt(conn->sock, IPPROTO_TCP, TCP_NODELAY,
				   (char *) &on,
				   sizeof(on)) < 0)
	{
		char		sebuf[PG_STRERROR_R_BUFLEN];

		db_append_conn_error(conn, "could not set socket to TCP no delay mode: %s",
								SOCK_STRERROR(SOCK_ERRNO, sebuf, sizeof(sebuf)));
		return 0;
	}
#endif

	return 1;
}


/*
 * We use these values for the "family" field.
 *
 * Referencing all of the non-AF_INET types to AF_INET lets us work on
 * machines which did not have the appropriate address family (like
 * inet6 addresses when AF_INET6 wasn't present) but didn't cause a
 * dump/reload requirement.  Pre-7.4 databases used AF_INET for the family
 * type on disk.
 */
#define PGSQL_AF_INET   (AF_INET + 0)
#define PGSQL_AF_INET6  (AF_INET + 1)
#define NS_IN6ADDRSZ 16
#define NS_INT16SZ 2
#define NS_INADDRSZ 4
#define SPRINTF(x) ((size_t)sprintf x)
static int
decoct(const u_char *src, int bytes, char *dst, size_t size)
{
	char	   *odst = dst;
	char	   *t;
	int			b;

	for (b = 1; b <= bytes; b++)
	{
		if (size <= sizeof "255.")
			return (0);
		t = dst;
		dst += SPRINTF((dst, "%u", *src++));
		if (b != bytes)
		{
			*dst++ = '.';
			*dst = '\0';
		}
		size -= (size_t) (dst - t);
	}
	return (dst - odst);
}

static char *
inet_net_ntop_ipv6(const u_char *src, int bits, char *dst, size_t size)
{
	/*
	 * Note that int32_t and int16_t need only be "at least" large enough to
	 * contain a value of the specified size.  On some systems, like Crays,
	 * there is no such thing as an integer variable with 16 bits. Keep this
	 * in mind if you think this function should have been coded to use
	 * pointer overlays.  All the world's not a VAX.
	 */
	char		tmp[sizeof "ffff:ffff:ffff:ffff:ffff:ffff:255.255.255.255/128"];
	char	   *tp;
	struct
	{
		int			base,
					len;
	}			best, cur;
	u_int		words[NS_IN6ADDRSZ / NS_INT16SZ];
	int			i;

	if ((bits < -1) || (bits > 128))
	{
		errno = EINVAL;
		return (NULL);
	}

	/*
	 * Preprocess: Copy the input (bytewise) array into a wordwise array. Find
	 * the longest run of 0x00's in src[] for :: shorthanding.
	 */
	memset(words, '\0', sizeof words);
	for (i = 0; i < NS_IN6ADDRSZ; i++)
		words[i / 2] |= (src[i] << ((1 - (i % 2)) << 3));
	best.base = -1;
	cur.base = -1;
	best.len = 0;
	cur.len = 0;
	for (i = 0; i < (NS_IN6ADDRSZ / NS_INT16SZ); i++)
	{
		if (words[i] == 0)
		{
			if (cur.base == -1)
				cur.base = i, cur.len = 1;
			else
				cur.len++;
		}
		else
		{
			if (cur.base != -1)
			{
				if (best.base == -1 || cur.len > best.len)
					best = cur;
				cur.base = -1;
			}
		}
	}
	if (cur.base != -1)
	{
		if (best.base == -1 || cur.len > best.len)
			best = cur;
	}
	if (best.base != -1 && best.len < 2)
		best.base = -1;

	/*
	 * Format the result.
	 */
	tp = tmp;
	for (i = 0; i < (NS_IN6ADDRSZ / NS_INT16SZ); i++)
	{
		/* Are we inside the best run of 0x00's? */
		if (best.base != -1 && i >= best.base &&
			i < (best.base + best.len))
		{
			if (i == best.base)
				*tp++ = ':';
			continue;
		}
		/* Are we following an initial run of 0x00s or any real hex? */
		if (i != 0)
			*tp++ = ':';
		/* Is this address an encapsulated IPv4? */
		if (i == 6 && best.base == 0 && (best.len == 6 ||
										 (best.len == 7 && words[7] != 0x0001) ||
										 (best.len == 5 && words[5] == 0xffff)))
		{
			int			n;

			n = decoct(src + 12, 4, tp, sizeof tmp - (tp - tmp));
			if (n == 0)
			{
				errno = EMSGSIZE;
				return (NULL);
			}
			tp += strlen(tp);
			break;
		}
		tp += SPRINTF((tp, "%x", words[i]));
	}

	/* Was it a trailing run of 0x00's? */
	if (best.base != -1 && (best.base + best.len) ==
		(NS_IN6ADDRSZ / NS_INT16SZ))
		*tp++ = ':';
	*tp = '\0';

	if (bits != -1 && bits != 128)
		tp += SPRINTF((tp, "/%u", bits));

	/*
	 * Check for overflow, copy, and we're done.
	 */
	if ((size_t) (tp - tmp) > size)
	{
		errno = EMSGSIZE;
		return (NULL);
	}
	strcpy(dst, tmp);
	return (dst);
}
/*
 * static char *
 * inet_net_ntop_ipv4(src, bits, dst, size)
 *	convert IPv4 network address from network to presentation format.
 *	"src"'s size is determined from its "af".
 * return:
 *	pointer to dst, or NULL if an error occurred (check errno).
 * note:
 *	network byte order assumed.  this means 192.5.5.240/28 has
 *	0b11110000 in its fourth octet.
 * author:
 *	Paul Vixie (ISC), October 1998
 */
static char *
inet_net_ntop_ipv4(const u_char *src, int bits, char *dst, size_t size)
{
	char	   *odst = dst;
	char	   *t;
	int			len = 4;
	int			b;

	if (bits < 0 || bits > 32)
	{
		errno = EINVAL;
		return (NULL);
	}

	/* Always format all four octets, regardless of mask length. */
	for (b = len; b > 0; b--)
	{
		if (size <= sizeof ".255")
			goto emsgsize;
		t = dst;
		if (dst != odst)
			*dst++ = '.';
		dst += SPRINTF((dst, "%u", *src++));
		size -= (size_t) (dst - t);
	}

	/* don't print masklen if 32 bits */
	if (bits != 32)
	{
		if (size <= sizeof "/32")
			goto emsgsize;
		dst += SPRINTF((dst, "/%u", bits));
	}

	return (odst);

emsgsize:
	errno = EMSGSIZE;
	return (NULL);
}


/*
 * char *
 * pg_inet_net_ntop(af, src, bits, dst, size)
 *	convert host/network address from network to presentation format.
 *	"src"'s size is determined from its "af".
 * return:
 *	pointer to dst, or NULL if an error occurred (check errno).
 * note:
 *	192.5.5.1/28 has a nonzero host part, which means it isn't a network
 *	as called for by pg_inet_net_pton() but it can be a host address with
 *	an included netmask.
 * author:
 *	Paul Vixie (ISC), October 1998
 */
char *
pg_inet_net_ntop(int af, const void *src, int bits, char *dst, size_t size)
{
	/*
	 * We need to cover both the address family constants used by the PG inet
	 * type (PGSQL_AF_INET and PGSQL_AF_INET6) and those used by the system
	 * libraries (AF_INET and AF_INET6).  We can safely assume PGSQL_AF_INET
	 * == AF_INET, but the INET6 constants are very likely to be different.
	 */
	switch (af)
	{
		case PGSQL_AF_INET:
			return (inet_net_ntop_ipv4(src, bits, dst, size));
		case PGSQL_AF_INET6:
#if AF_INET6 != PGSQL_AF_INET6
		case AF_INET6:
#endif
			return (inet_net_ntop_ipv6(src, bits, dst, size));
		default:
			errno = EAFNOSUPPORT;
			return (NULL);
	}
}

/* ----------
 * Write currently connected IP address into host_addr (of len host_addr_len).
 * If unable to, set it to the empty string.
 * ----------
 */
static void
getHostaddr(Conn *conn, char *host_addr, int host_addr_len)
{
	struct sockaddr_storage *addr = &conn->raddr.addr;

	if (addr->ss_family == AF_INET)
	{
		if (pg_inet_net_ntop(AF_INET,
							 &((struct sockaddr_in *) addr)->sin_addr.s_addr,
							 32,
							 host_addr, host_addr_len) == NULL)
			host_addr[0] = '\0';
	}
	else if (addr->ss_family == AF_INET6)
	{
		if (pg_inet_net_ntop(AF_INET6,
							 &((struct sockaddr_in6 *) addr)->sin6_addr.s6_addr,
							 128,
							 host_addr, host_addr_len) == NULL)
			host_addr[0] = '\0';
	}
	else
		host_addr[0] = '\0';
}

/* ----------
 * connectFailureMessage -
 * create a friendly error message on connection failure,
 * using the given errno value.  Use this for error cases that
 * imply that there's no server there.
 * ----------
 */
static void
connectFailureMessage(Conn *conn, int errorno)
{
	char		sebuf[PG_STRERROR_R_BUFLEN];

	appendExpBuffer(&conn->errorMessage,
					  "%s\n",
					  SOCK_STRERROR(errorno, sebuf, sizeof(sebuf)));

	if (conn->raddr.addr.ss_family == AF_UNIX)
		db_append_conn_error(conn, "\tIs the server running locally and accepting connections on that socket?");
	else
		db_append_conn_error(conn, "\tIs the server running on that host and accepting TCP/IP connections?");
}


/* ----------
 * ConnectStart -
 *		Begin the process of making a connection to the backend.
 *
 * Returns 1 if successful, 0 if not.
 * ----------
 */
int
ConnectStart(Conn *conn)
{
	if (!conn)
		return 0;

	if (!conn->options_valid)
		goto connect_errReturn;


	/* Ensure our buffers are empty */
	conn->inStart = conn->inCursor = conn->inEnd = 0;
	conn->outCount = 0;
	conn->whichhost = -1;
	conn->try_next_host = true;
	conn->try_next_addr = false;

	conn->status = CONNECTION_NEEDED;


	/*
	 * The code for processing CONNECTION_NEEDED state is in connectPoll(),
	 * so that it can easily be re-executed if needed again during the
	 * asynchronous startup process.  However, we must run it once here,
	 * because callers expect a success return from this routine to mean that
	 * we are in PGRES_POLLING_WRITING connection state.
	 */
	if (connectPoll(conn))
		return 1;

connect_errReturn:
	conn->status = CONNECTION_BAD;
	return 0;
}


/*
 * BSD-style getpeereid() for platforms that lack it.
 */
int
getpeereid(int sock, uid_t *uid, gid_t *gid)
{
#if defined(SO_PEERCRED)
	/* Linux: use getsockopt(SO_PEERCRED) */
	struct ucred peercred;
	socklen_t	so_len = sizeof(peercred);

	if (getsockopt(sock, SOL_SOCKET, SO_PEERCRED, &peercred, &so_len) != 0 ||
		so_len != sizeof(peercred))
		return -1;
	*uid = peercred.uid;
	*gid = peercred.gid;
	return 0;
#elif defined(LOCAL_PEERCRED)
	/* Debian with FreeBSD kernel: use getsockopt(LOCAL_PEERCRED) */
	struct xucred peercred;
	socklen_t	so_len = sizeof(peercred);

	if (getsockopt(sock, 0, LOCAL_PEERCRED, &peercred, &so_len) != 0 ||
		so_len != sizeof(peercred) ||
		peercred.cr_version != XUCRED_VERSION)
		return -1;
	*uid = peercred.cr_uid;
	*gid = peercred.cr_gid;
	return 0;
#elif defined(HAVE_GETPEERUCRED)
	/* Solaris: use getpeerucred() */
	ucred_t    *ucred;

	ucred = NULL;				/* must be initialized to NULL */
	if (getpeerucred(sock, &ucred) == -1)
		return -1;

	*uid = ucred_geteuid(ucred);
	*gid = ucred_getegid(ucred);
	ucred_free(ucred);

	if (*uid == (uid_t) (-1) || *gid == (gid_t) (-1))
		return -1;
	return 0;
#else
	/* No implementation available on this platform */
	errno = ENOSYS;
	return -1;
#endif
}


/*
 *	pg_getaddrinfo_all - get address info for Unix, IPv4 and IPv6 sockets
 */
int
pg_getaddrinfo_all(const char *hostname, const char *servname,
				   const struct addrinfo *hintp, struct addrinfo **result)
{
	int			rc;

	/* not all versions of getaddrinfo() zero *result on failure */
	*result = NULL;


	/* NULL has special meaning to getaddrinfo(). */
	rc = getaddrinfo((!hostname || hostname[0] == '\0') ? NULL : hostname,
					 servname, hintp, result);

	return rc;
}



/* ----------------
 *		connectPoll
 *
 * Poll an asynchronous connection.
 *
 * Before calling this function, use select(2) to determine when data
 * has arrived..
 *
 * You must call finish whether or not this fails.
 *
 * This function and connectStart are intended to allow connections to be
 * made without blocking the execution of your program on remote I/O. However,
 * there are a number of caveats:
 *
 *	 o	If you call trace, ensure that the stream object into which you trace
 *		will not block.
 *	 o	If you do not supply an IP address for the remote host (i.e. you
 *		supply a host name instead) then connectStart will block on
 *		getaddrinfo.  You will be fine if using Unix sockets (i.e. by
 *		supplying neither a host name nor a host address).
 *	 o	If your backend wants to use Kerberos authentication then you must
 *		supply both a host name and a host address, otherwise this function
 *		may block on gethostname.
 *
 * ----------------
 */

bool
connectPoll(Conn *conn)
{
	char		sebuf[PG_STRERROR_R_BUFLEN];
	int			optval;
	int			save_whichhost;
	int			save_whichaddr;

	if (conn == NULL)
		return false;

	save_whichhost = conn->whichhost;
	save_whichaddr = conn->whichaddr;

	for (; conn->whichhost < conn->nconnhost; conn->whichhost++)
	{
		pg_conn_host *ch;
		struct addrinfo hint;
		struct addrinfo *addrlist;
		int			thisport;
		int			ret;
		char		portstr[MAXPGPATH];

		ch = &conn->connhost[conn->whichhost];
		MemSet(&hint, 0, sizeof(hint));
		hint.ai_socktype = SOCK_STREAM;
		hint.ai_family = AF_UNSPEC;

		if (ch->port == NULL || ch->port[0] == '\0')
			thisport = DEF_PGPORT;
		else
		{
			if (!ParseIntParam(ch->port, &thisport, conn, "port"))
				goto error_return;

			if (thisport < 1 || thisport > 65535)
			{
				db_append_conn_error(conn, "invalid port number: \"%s\"", ch->port);
				continue;
			}
		}
		snprintf(portstr, sizeof(portstr), "%d", thisport);

		switch (ch->type)
		{
			case CHT_HOST_NAME:
				ret = pg_getaddrinfo_all(ch->host, portstr, &hint, &addrlist);
				if (ret || !addrlist)
				{
					db_append_conn_error(conn, "could not translate host name \"%s\" to address: %s",
										 ch->host, gai_strerror(ret));
					continue;
				}
				break;

			case CHT_HOST_ADDRESS:
				hint.ai_flags = AI_NUMERICHOST;
				ret = pg_getaddrinfo_all(ch->hostaddr, portstr, &hint, &addrlist);
				if (ret || !addrlist)
				{
					db_append_conn_error(conn, "could not parse network address \"%s\": %s",
										 ch->hostaddr, gai_strerror(ret));
					continue;
				}
				break;

			case CHT_UNIX_SOCKET:
				hint.ai_family = AF_UNIX;
				UNIXSOCK_PATH(portstr, thisport, ch->host);
				ret = pg_getaddrinfo_all(NULL, portstr, &hint, &addrlist);
				if (ret || !addrlist)
				{
					db_append_conn_error(conn, "could not translate Unix-domain socket path \"%s\" to address: %s",
										 portstr, gai_strerror(ret));
					continue;
				}
				break;

			default:
				continue;
		}

		if (store_conn_addrinfo(conn, addrlist))
		{
			//pg_freeaddrinfo_all(hint.ai_family, addrlist);
			continue;
		}

		for (conn->whichaddr = (conn->whichhost == save_whichhost) ? save_whichaddr : 0;
			 conn->whichaddr < conn->naddr;
			 conn->whichaddr++)
		{
			char		host_addr[NI_MAXHOST];
			int			sock_type;
			AddrInfo   *addr_cur;

			addr_cur = &conn->addr[conn->whichaddr];
			memcpy(&conn->raddr, &addr_cur->addr, sizeof(SockAddr));

			if (conn->connip != NULL)
			{
				free(conn->connip);
				conn->connip = NULL;
			}
			getHostaddr(conn, host_addr, NI_MAXHOST);
			if (host_addr[0])
				conn->connip = strdup(host_addr);

			sock_type = SOCK_STREAM;
#ifdef SOCK_CLOEXEC
			sock_type |= SOCK_CLOEXEC;
#endif
			conn->sock = socket(addr_cur->family, sock_type, 0);
			if (conn->sock == PGINVALID_SOCKET)
			{
				int			errorno = SOCK_ERRNO;
				db_append_conn_error(conn, "could not create socket: %s",
									 SOCK_STRERROR(errorno, sebuf, sizeof(sebuf)));
				continue;
			}

			if (addr_cur->family != AF_UNIX && !connectNoDelay(conn))
			{
				close(conn->sock);
				conn->sock = PGINVALID_SOCKET;
				continue;
			}

#ifndef SOCK_NONBLOCK
		//	if (!pg_set_noblock(conn->sock))
		//	{
		//		db_append_conn_error(conn, "could not set socket to nonblocking mode: %s",
		//							 SOCK_STRERROR(SOCK_ERRNO, sebuf, sizeof(sebuf)));
		//		close(conn->sock);
		//		conn->sock = PGINVALID_SOCKET;
		//		continue;
		//	}
#endif
int flags = fcntl(conn->sock, F_GETFL, 0);
fcntl(conn->sock, F_SETFL, flags & ~O_NONBLOCK);  // Disable non-blocking mode


#ifndef SOCK_CLOEXEC
#ifdef F_SETFD
			if (fcntl(conn->sock, F_SETFD, FD_CLOEXEC) == -1)
			{
				db_append_conn_error(conn, "could not set socket to close-on-exec mode: %s",
									 SOCK_STRERROR(SOCK_ERRNO, sebuf, sizeof(sebuf)));
				close(conn->sock);
				conn->sock = PGINVALID_SOCKET;
				continue;
			}
#endif
#endif

			conn->sigpipe_so = false;
#ifdef MSG_NOSIGNAL
			conn->sigpipe_flag = true;
#else
			conn->sigpipe_flag = false;
#endif

#ifdef SO_NOSIGPIPE
			optval = 1;
			if (setsockopt(conn->sock, SOL_SOCKET, SO_NOSIGPIPE,
						   (char *) &optval, sizeof(optval)) == 0)
			{
				conn->sigpipe_so = true;
				conn->sigpipe_flag = false;
			}
#endif

			while (connect(conn->sock, (struct sockaddr *) &addr_cur->addr.addr,
						   addr_cur->addr.salen) < 0)
			{
				if (errno == EINTR)
					continue;
				connectFailureMessage(conn, errno);
				close(conn->sock);
				conn->sock = PGINVALID_SOCKET;
				break;
			}

			if (conn->sock == PGINVALID_SOCKET)
				continue;

			socklen_t	optlen = sizeof(optval);
			if (getsockopt(conn->sock, SOL_SOCKET, SO_ERROR,
						   (char *) &optval, &optlen) == -1)
			{
				db_append_conn_error(conn, "could not get socket error status: %s",
									 SOCK_STRERROR(SOCK_ERRNO, sebuf, sizeof(sebuf)));
				close(conn->sock);
				conn->sock = PGINVALID_SOCKET;
				continue;
			}
			else if (optval != 0)
			{
				connectFailureMessage(conn, optval);
				close(conn->sock);
				conn->sock = PGINVALID_SOCKET;
				continue;
			}

			conn->laddr.salen = sizeof(conn->laddr.addr);
			if (getsockname(conn->sock,
							(struct sockaddr *) &conn->laddr.addr,
							&conn->laddr.salen) < 0)
			{
				db_append_conn_error(conn, "could not get client address from socket: %s",
									 SOCK_STRERROR(SOCK_ERRNO, sebuf, sizeof(sebuf)));
				close(conn->sock);
				conn->sock = PGINVALID_SOCKET;
				continue;
			}

			if (conn->requirepeer && conn->requirepeer[0] &&
				conn->raddr.addr.ss_family == AF_UNIX)
			{
				uid_t		uid;
				gid_t		gid;

				errno = 0;
				if (getpeereid(conn->sock, &uid, &gid) != 0)
				{
					if (errno == ENOSYS)
						db_append_conn_error(conn, "requirepeer parameter is not supported on this platform");
					else
						db_append_conn_error(conn, "could not get peer credentials: %s",
											 strerror_r(errno, sebuf, sizeof(sebuf)));
					close(conn->sock);
					conn->sock = PGINVALID_SOCKET;
					continue;
				}
				Assert(false);
			}

			PollingStatusType gss_status;
			bool		gss_done = false;
			bool		gss_success = false;
			

			while (!gss_done)
			{
				gss_status = pqsecure_open_gss(conn);

				switch (gss_status)
				{
					case PGRES_POLLING_OK:
						gss_done = true;
						gss_success = true;
						break;

					case PGRES_POLLING_READING:
					case PGRES_POLLING_WRITING:
						{
							fd_set		read_fds;
							fd_set		write_fds;
							int			nfds = conn->sock + 1;

							FD_ZERO(&read_fds);
							FD_ZERO(&write_fds);

							if (gss_status == PGRES_POLLING_READING)
								FD_SET(conn->sock, &read_fds);
							else
								FD_SET(conn->sock, &write_fds);

							if (select(nfds, &read_fds, &write_fds, NULL, NULL) < 0)
							{
								if (errno == EINTR)
									continue;
								db_append_conn_error(conn, "select() failed: %s",
													 strerror(errno));
								gss_done = true;
							}
						}
						break;

					case PGRES_POLLING_FAILED:
						gss_done = true;
						break;

					default:
						db_append_conn_error(conn, "unexpected GSSAPI polling status: %d",
											 (int) gss_status);
						gss_done = true;
						break;
				}
			}

			if (gss_success)
			{
				conn->status = CONNECTION_STARTED;
				return true;
			}
			else
			{
				close(conn->sock);
				conn->sock = PGINVALID_SOCKET;
			}
		}
	}

error_return:
	conn->whichhost = save_whichhost;
	conn->whichaddr = save_whichaddr;
	conn->status = CONNECTION_BAD;
	return false;
}

/*
 * MakeEmptyConn
 *	 - create a Conn data structure with (as yet) no interesting data
 */
Conn *
MakeEmptyConn(void)
{
	Conn	   *conn;

							/* WIN32 */

	conn = (Conn *) malloc(sizeof(Conn));
	if (conn == NULL)
		return conn;

	/* Zero all pointers and booleans */
	MemSet(conn, 0, sizeof(Conn));

	conn->status = CONNECTION_BAD;
	conn->options_valid = false;
	conn->nonblocking = false;
	conn->std_strings = false;	/* unless server says differently */
	conn->default_transaction_read_only = PG_BOOL_UNKNOWN;
	conn->in_hot_standby = PG_BOOL_UNKNOWN;
	conn->sock = PGINVALID_SOCKET;
	conn->Pfdebug = NULL;

	/*
	 * We try to send at least 8K at a time, which is the usual size of pipe
	 * buffers on Unix systems.  That way, when we are sending a large amount
	 * of data, we avoid incurring extra kernel context swaps for partial
	 * bufferloads.  The output buffer is initially made 16K in size, and we
	 * try to dump it after accumulating 8K.
	 *
	 * With the same goal of minimizing context swaps, the input buffer will
	 * be enlarged anytime it has less than 8K free, so we initially allocate
	 * twice that.
	 */
	conn->inBufSize = 16 * 1024;
	conn->inBuffer = (char *) malloc(conn->inBufSize);
	conn->outBufSize = 16 * 1024;
	conn->outBuffer = (char *) malloc(conn->outBufSize);
	conn->rowBufLen = 32;
	conn->rowBuf = (PGdataValue *) malloc(conn->rowBufLen * sizeof(PGdataValue));
	initExpBuffer(&conn->errorMessage);
	initExpBuffer(&conn->workBuffer);

	if (conn->inBuffer == NULL ||
		conn->outBuffer == NULL ||
		conn->rowBuf == NULL ||
		ExpBufferBroken(&conn->errorMessage) ||
		ExpBufferBroken(&conn->workBuffer))
	{
		conn = NULL;
	}

	return conn;
}


/*
 * store_conn_addrinfo
 *	 - copy addrinfo to Conn object
 *
 * Copies the addrinfos from addrlist to the Conn object such that the
 * addrinfos can be manipulated by libpq. Returns a positive integer on
 * failure, otherwise zero.
 */
static int
store_conn_addrinfo(Conn *conn, struct addrinfo *addrlist)
{
	struct addrinfo *ai = addrlist;

	conn->whichaddr = 0;

	conn->naddr = 0;
	while (ai)
	{
		ai = ai->ai_next;
		conn->naddr++;
	}

	conn->addr = calloc(conn->naddr, sizeof(AddrInfo));
	if (conn->addr == NULL)
	{
		db_append_conn_error(conn, "out of memory");
		return 1;
	}

	ai = addrlist;
	for (int i = 0; i < conn->naddr; i++)
	{
		conn->addr[i].family = ai->ai_family;

		memcpy(&conn->addr[i].addr.addr, ai->ai_addr,
			   ai->ai_addrlen);
		conn->addr[i].addr.salen = ai->ai_addrlen;
		ai = ai->ai_next;
	}

	return 0;
}




/*
 *		conninfoParse
 *
 * Parse a string like connectdb() would do and return the
 * resulting connection options array.  NULL is returned on failure.
 * The result contains only options specified directly in the string,
 * not any possible default values.
 *
 * If errmsg isn't NULL, *errmsg is set to NULL on success, or a malloc'd
 * string on failure (use freemem to free it).  In out-of-memory conditions
 * both *errmsg and the result could be NULL.
 *
 * NOTE: the returned array is dynamically allocated and should
 * be freed when no longer needed via conninfoFree().
 */
conninfoOption *
conninfoParse(const char *conninfo, char **errmsg)
{
	ExpBufferData errorBuf;
	conninfoOption *connOptions;

	if (errmsg)
		*errmsg = NULL;			/* default */
	initExpBuffer(&errorBuf);
	if (ExpBufferDataBroken(errorBuf))
		return NULL;			/* out of memory already :-( */
	connOptions = parse_connection_string(conninfo, &errorBuf, false);
	if (connOptions == NULL && errmsg)
		*errmsg = errorBuf.data;
	else
		termExpBuffer(&errorBuf);
	return connOptions;
}

/*
 * Build a working copy of the constant conninfoOptions array.
 */
static conninfoOption *
conninfo_init(ExpBuffer errorMessage)
{
	conninfoOption *options;
	conninfoOption *opt_dest;
	const internalconninfoOption *cur_opt;

	/*
	 * Get enough memory for all options in conninfoOptions, even if some
	 * end up being filtered out.
	 */
	options = (conninfoOption *) malloc(sizeof(conninfoOption) * sizeof(conninfoOptions) / sizeof(conninfoOptions[0]));
	if (options == NULL)
	{
		db_append_error(errorMessage, "out of memory");
		return NULL;
	}
	opt_dest = options;

	for (cur_opt = conninfoOptions; cur_opt->keyword; cur_opt++)
	{
		/* Only copy the public part of the struct, not the full internal */
		memcpy(opt_dest, cur_opt, sizeof(conninfoOption));
		opt_dest++;
	}
	MemSet(opt_dest, 0, sizeof(conninfoOption));

	return options;
}

/*
 * Connection string parser
 *
 * Returns a malloc'd conninfoOption array, if parsing is successful.
 * Otherwise, NULL is returned and an error message is added to errorMessage.
 *
 * If use_defaults is true, default values are filled in (from a service file,
 * environment variables, etc).
 */
static conninfoOption *
parse_connection_string(const char *connstr, ExpBuffer errorMessage,
						bool use_defaults)
{
	/* Parse as URI if connection string matches URI prefix */
	if (uri_prefix_length(connstr) != 0)
		return conninfo_uri_parse(connstr, errorMessage, use_defaults);

	/* Parse as default otherwise */
	return conninfo_parse(connstr, errorMessage, use_defaults);
}

/*
 * Checks if connection string starts with either of the valid URI prefix
 * designators.
 *
 * Returns the URI prefix length, 0 if the string doesn't contain a URI prefix.
 *
 * XXX this is duplicated in psql/common.c.
 */
static int
uri_prefix_length(const char *connstr)
{
	if (strncmp(connstr, uri_designator,
				sizeof(uri_designator) - 1) == 0)
		return sizeof(uri_designator) - 1;

	if (strncmp(connstr, short_uri_designator,
				sizeof(short_uri_designator) - 1) == 0)
		return sizeof(short_uri_designator) - 1;

	return 0;
}

/*
 * Subroutine for parse_connection_string
 *
 * Deal with a string containing key=value pairs.
 */
static conninfoOption *
conninfo_parse(const char *conninfo, ExpBuffer errorMessage,
			   bool use_defaults)
{
	char	   *pname;
	char	   *pval;
	char	   *buf;
	char	   *cp;
	char	   *cp2;
	conninfoOption *options;

	/* Make a working copy of conninfoOptions */
	options = conninfo_init(errorMessage);
	if (options == NULL)
		return NULL;

	/* Need a modifiable copy of the input string */
	if ((buf = strdup(conninfo)) == NULL)
	{
		db_append_error(errorMessage, "out of memory");
		return NULL;
	}
	cp = buf;

	while (*cp)
	{
		/* Skip blanks before the parameter name */
		if (isspace((unsigned char) *cp))
		{
			cp++;
			continue;
		}

		/* Get the parameter name */
		pname = cp;
		while (*cp)
		{
			if (*cp == '=')
				break;
			if (isspace((unsigned char) *cp))
			{
				*cp++ = '\0';
				while (*cp)
				{
					if (!isspace((unsigned char) *cp))
						break;
					cp++;
				}
				break;
			}
			cp++;
		}

		/* Check that there is a following '=' */
		if (*cp != '=')
		{
			db_append_error(errorMessage,
							   "missing \"=\" after \"%s\" in connection info string",
							   pname);
			free(buf);
			return NULL;
		}
		*cp++ = '\0';

		/* Skip blanks after the '=' */
		while (*cp)
		{
			if (!isspace((unsigned char) *cp))
				break;
			cp++;
		}

		/* Get the parameter value */
		pval = cp;

		if (*cp != '\'')
		{
			cp2 = pval;
			while (*cp)
			{
				if (isspace((unsigned char) *cp))
				{
					*cp++ = '\0';
					break;
				}
				if (*cp == '\\')
				{
					cp++;
					if (*cp != '\0')
						*cp2++ = *cp++;
				}
				else
					*cp2++ = *cp++;
			}
			*cp2 = '\0';
		}
		else
		{
			cp2 = pval;
			cp++;
			for (;;)
			{
				if (*cp == '\0')
				{
					db_append_error(errorMessage, "unterminated quoted string in connection info string");
					free(buf);
					return NULL;
				}
				if (*cp == '\\')
				{
					cp++;
					if (*cp != '\0')
						*cp2++ = *cp++;
					continue;
				}
				if (*cp == '\'')
				{
					*cp2 = '\0';
					cp++;
					break;
				}
				*cp2++ = *cp++;
			}
		}

		/*
		 * Now that we have the name and the value, store the record.
		 */
		if (!conninfo_storeval(options, pname, pval, errorMessage, false, false))
		{
			free(buf);
			return NULL;
		}
	}

	/* Done with the modifiable input string */
	free(buf);

	/*
	 * Add in defaults if the caller wants that.
	 */
	if (use_defaults)
	{
		if (!conninfo_add_defaults(options, errorMessage))
		{
			return NULL;
		}
	}

	return options;
}

/*
 * Modified Conninfo array parser routine to extract only user, password, host, and port.
 */
static conninfoOption *
conninfo_array_parse(const char *const *keywords, const char *const *values,
					 ExpBuffer errorMessage, bool use_defaults)
{
	conninfoOption *options;
	conninfoOption *option;
	int			i = 0;

	/* Make a working copy of conninfoOptions */
	options = conninfo_init(errorMessage);
	if (options == NULL)
	{
		return NULL;
	}

	/* Parse the keywords/values arrays */
	i = 0;
	while (keywords[i])
	{
		const char *pname = keywords[i];
		const char *pvalue = values[i];

		if (pvalue != NULL && pvalue[0] != '\0')
		{
			/* Search for the param record */
			for (option = options; option->keyword != NULL; option++)
			{
				if (strcmp(option->keyword, pname) == 0)
					break;
			}

			/* Skip unrecognized keywords */
			if (option->keyword == NULL)
			{
				++i;
				continue;
			}

			/* Process only user, password, host, and port */
			if (strcmp(pname, "user") != 0 &&
				strcmp(pname, "password") != 0 &&
				strcmp(pname, "host") != 0 &&
				strcmp(pname, "port") != 0)
			{
				++i;
				continue;
			}

			/* Store the value */
			free(option->val);
			option->val = strdup(pvalue);
			if (!option->val)
			{
				db_append_error(errorMessage, "out of memory");
				return NULL;
			}
		}
		++i;
	}

	/* Apply defaults if required */
	if (use_defaults)
	{
		if (!conninfo_add_defaults(options, errorMessage))
		{
			return NULL;
		}
	}

	/* Clear all non-target parameters */
	for (option = options; option->keyword != NULL; option++)
	{
		if (strcmp(option->keyword, "user") != 0 &&
			strcmp(option->keyword, "password") != 0 &&
			strcmp(option->keyword, "host") != 0 &&
			strcmp(option->keyword, "port") != 0)
		{
			free(option->val);
			option->val = NULL;
		}
	}

	return options;
}
/*
 * Add the default values for any unspecified options to the connection
 * options array.
 *
 * Defaults are obtained from a service file, environment variables, etc.
 *
 * Returns true if successful, otherwise false; errorMessage, if supplied,
 * is filled in upon failure.  Note that failure to locate a default value
 * is not an error condition here --- we just leave the option's value as
 * NULL.
 */
static bool
conninfo_add_defaults(conninfoOption *options, ExpBuffer errorMessage)
{
	conninfoOption *option;
	char	   *tmp;

	/*
	 * Get the fallback resources for parameters not specified in the conninfo
	 * string nor the service.
	 */
	for (option = options; option->keyword != NULL; option++)
	{
		if (option->val != NULL)
			continue;			/* Value was in conninfo or service */

		/*
		 * Try to get the environment variable fallback
		 */
		if (option->envvar != NULL)
		{
			if ((tmp = getenv(option->envvar)) != NULL)
			{
				option->val = strdup(tmp);
				if (!option->val)
				{
					if (errorMessage)
						db_append_error(errorMessage, "out of memory");
					return false;
				}
				continue;
			}
		}


		/*
		 * No environment variable specified or the variable isn't set - try
		 * compiled-in default
		 */
		if (option->compiled != NULL)
		{
			option->val = strdup(option->compiled);
			if (!option->val)
			{
				if (errorMessage)
					db_append_error(errorMessage, "out of memory");
				return false;
			}
			continue;
		}

	}


	return true;
}

/*
 * Subroutine for parse_connection_string
 *
 * Deal with a URI connection string.
 */
static conninfoOption *
conninfo_uri_parse(const char *uri, ExpBuffer errorMessage,
				   bool use_defaults)
{
	conninfoOption *options;

	/* Make a working copy of conninfoOptions */
	options = conninfo_init(errorMessage);
	if (options == NULL)
		return NULL;

	if (!conninfo_uri_parse_options(options, uri, errorMessage))
	{
		return NULL;
	}

	/*
	 * Add in defaults if the caller wants that.
	 */
	if (use_defaults)
	{
		if (!conninfo_add_defaults(options, errorMessage))
		{
			return NULL;
		}
	}

	return options;
}

/*
 * conninfo_uri_parse_options
 *		Actual URI parser.
 *
 * If successful, returns true while the options array is filled with parsed
 * options from the URI.
 * If not successful, returns false and fills errorMessage accordingly.
 *
 * Parses the connection URI string in 'uri' according to the URI syntax (RFC
 * 3986):
 *
 * postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&...]
 *
 * where "netloc" is a hostname, an IPv4 address, or an IPv6 address surrounded
 * by literal square brackets.  As an extension, we also allow multiple
 * netloc[:port] specifications, separated by commas:
 *
 * postgresql://[user[:password]@][netloc][:port][,...][/dbname][?param1=value1&...]
 *
 * Any of the URI parts might use percent-encoding (%xy).
 */
static bool
conninfo_uri_parse_options(conninfoOption *options, const char *uri,
						   ExpBuffer errorMessage)
{
	int			prefix_len;
	char	   *p;
	char	   *buf = NULL;
	char	   *start;
	char		prevchar = '\0';
	char	   *user = NULL;
	char	   *host = NULL;
	bool		retval = false;
	ExpBufferData hostbuf;
	ExpBufferData portbuf;

	initExpBuffer(&hostbuf);
	initExpBuffer(&portbuf);
	if (ExpBufferDataBroken(hostbuf) || ExpBufferDataBroken(portbuf))
	{
		db_append_error(errorMessage, "out of memory");
		goto cleanup;
	}

	/* need a modifiable copy of the input URI */
	buf = strdup(uri);
	if (buf == NULL)
	{
		db_append_error(errorMessage, "out of memory");
		goto cleanup;
	}
	start = buf;

	/* Skip the URI prefix */
	prefix_len = uri_prefix_length(uri);
	if (prefix_len == 0)
	{
		/* Should never happen */
		db_append_error(errorMessage,
						   "invalid URI propagated to internal parser routine: \"%s\"",
						   uri);
		goto cleanup;
	}
	start += prefix_len;
	p = start;

	/* Look ahead for possible user credentials designator */
	while (*p && *p != '@' && *p != '/')
		++p;
	if (*p == '@')
	{
		/*
		 * Found username/password designator, so URI should be of the form
		 * "scheme://user[:password]@[netloc]".
		 */
		user = start;

		p = user;
		while (*p != ':' && *p != '@')
			++p;

		/* Save last char and cut off at end of user name */
		prevchar = *p;
		*p = '\0';

		if (*user &&
			!conninfo_storeval(options, "user", user,
							   errorMessage, false, true))
			goto cleanup;

		if (prevchar == ':')
		{
			const char *password = p + 1;

			while (*p != '@')
				++p;
			*p = '\0';

			if (*password &&
				!conninfo_storeval(options, "password", password,
								   errorMessage, false, true))
				goto cleanup;
		}

		/* Advance past end of parsed user name or password token */
		++p;
	}
	else
	{
		/*
		 * No username/password designator found.  Reset to start of URI.
		 */
		p = start;
	}

	/*
	 * There may be multiple netloc[:port] pairs, each separated from the next
	 * by a comma.  When we initially enter this loop, "p" has been
	 * incremented past optional URI credential information at this point and
	 * now points at the "netloc" part of the URI.  On subsequent loop
	 * iterations, "p" has been incremented past the comma separator and now
	 * points at the start of the next "netloc".
	 */
	for (;;)
	{
		/*
		 * Look for IPv6 address.
		 */
		if (*p == '[')
		{
			host = ++p;
			while (*p && *p != ']')
				++p;
			if (!*p)
			{
				db_append_error(errorMessage,
								   "end of string reached when looking for matching \"]\" in IPv6 host address in URI: \"%s\"",
								   uri);
				goto cleanup;
			}
			if (p == host)
			{
				db_append_error(errorMessage,
								   "IPv6 host address may not be empty in URI: \"%s\"",
								   uri);
				goto cleanup;
			}

			/* Cut off the bracket and advance */
			*(p++) = '\0';

			/*
			 * The address may be followed by a port specifier or a slash or a
			 * query or a separator comma.
			 */
			if (*p && *p != ':' && *p != '/' && *p != '?' && *p != ',')
			{
				db_append_error(errorMessage,
								   "unexpected character \"%c\" at position %d in URI (expected \":\" or \"/\"): \"%s\"",
								   *p, (int) (p - buf + 1), uri);
				goto cleanup;
			}
		}
		else
		{
			/* not an IPv6 address: DNS-named or IPv4 netloc */
			host = p;

			/*
			 * Look for port specifier (colon) or end of host specifier
			 * (slash) or query (question mark) or host separator (comma).
			 */
			while (*p && *p != ':' && *p != '/' && *p != '?' && *p != ',')
				++p;
		}

		/* Save the hostname terminator before we null it */
		prevchar = *p;
		*p = '\0';

		appendExpBufferStr(&hostbuf, host);

		if (prevchar == ':')
		{
			const char *port = ++p; /* advance past host terminator */

			while (*p && *p != '/' && *p != '?' && *p != ',')
				++p;

			prevchar = *p;
			*p = '\0';

			appendExpBufferStr(&portbuf, port);
		}

		if (prevchar != ',')
			break;
		++p;					/* advance past comma separator */
		appendExpBufferChar(&hostbuf, ',');
		appendExpBufferChar(&portbuf, ',');
	}

	/* Save final values for host and port. */
	if (ExpBufferDataBroken(hostbuf) || ExpBufferDataBroken(portbuf))
		goto cleanup;
	if (hostbuf.data[0] &&
		!conninfo_storeval(options, "host", hostbuf.data,
						   errorMessage, false, true))
		goto cleanup;
	if (portbuf.data[0] &&
		!conninfo_storeval(options, "port", portbuf.data,
						   errorMessage, false, true))
		goto cleanup;

	if (prevchar && prevchar != '?')
	{
		const char *dbname = ++p;	/* advance past host terminator */

		/* Look for query parameters */
		while (*p && *p != '?')
			++p;

		prevchar = *p;
		*p = '\0';

		/*
		 * Avoid setting dbname to an empty string, as it forces the default
		 * value (username) and ignores $PGDATABASE, as opposed to not setting
		 * it at all.
		 */
		if (*dbname &&
			!conninfo_storeval(options, "dbname", dbname,
							   errorMessage, false, true))
			goto cleanup;
	}

	if (prevchar)
	{
		++p;					/* advance past terminator */

		if (!conninfo_uri_parse_params(p, options, errorMessage))
			goto cleanup;
	}

	/* everything parsed okay */
	retval = true;

cleanup:
	termExpBuffer(&hostbuf);
	termExpBuffer(&portbuf);
	free(buf);
	return retval;
}

/*
 * Connection URI parameters parser routine
 *
 * If successful, returns true while connOptions is filled with parsed
 * parameters.  Otherwise, returns false and fills errorMessage appropriately.
 *
 * Destructively modifies 'params' buffer.
 */
static bool
conninfo_uri_parse_params(char *params,
						  conninfoOption *connOptions,
						  ExpBuffer errorMessage)
{
	while (*params)
	{
		char	   *keyword = params;
		char	   *value = NULL;
		char	   *p = params;
		bool		malloced = false;
		int			oldmsglen;

		/*
		 * Scan the params string for '=' and '&', marking the end of keyword
		 * and value respectively.
		 */
		for (;;)
		{
			if (*p == '=')
			{
				/* Was there '=' already? */
				if (value != NULL)
				{
					db_append_error(errorMessage,
									   "extra key/value separator \"=\" in URI query parameter: \"%s\"",
									   keyword);
					return false;
				}
				/* Cut off keyword, advance to value */
				*p++ = '\0';
				value = p;
			}
			else if (*p == '&' || *p == '\0')
			{
				/*
				 * If not at the end, cut off value and advance; leave p
				 * pointing to start of the next parameter, if any.
				 */
				if (*p != '\0')
					*p++ = '\0';
				/* Was there '=' at all? */
				if (value == NULL)
				{
					db_append_error(errorMessage,
									   "missing key/value separator \"=\" in URI query parameter: \"%s\"",
									   keyword);
					return false;
				}
				/* Got keyword and value, go process them. */
				break;
			}
			else
				++p;			/* Advance over all other bytes. */
		}

		keyword = conninfo_uri_decode(keyword, errorMessage);
		if (keyword == NULL)
		{
			/* conninfo_uri_decode already set an error message */
			return false;
		}
		value = conninfo_uri_decode(value, errorMessage);
		if (value == NULL)
		{
			/* conninfo_uri_decode already set an error message */
			free(keyword);
			return false;
		}
		malloced = true;

		/*
		 * Special keyword handling for improved JDBC compatibility.
		 */
		if (strcmp(keyword, "ssl") == 0 &&
			strcmp(value, "true") == 0)
		{
			free(keyword);
			free(value);
			malloced = false;

			keyword = "sslmode";
			value = "require";
		}

		/*
		 * Store the value if the corresponding option exists; ignore
		 * otherwise.  At this point both keyword and value are not
		 * URI-encoded.
		 */
		oldmsglen = errorMessage->len;
		if (!conninfo_storeval(connOptions, keyword, value,
							   errorMessage, true, false))
		{
			/* Insert generic message if conninfo_storeval didn't give one. */
			if ((int)errorMessage->len == oldmsglen)
				db_append_error(errorMessage,
								   "invalid URI query parameter: \"%s\"",
								   keyword);
			/* And fail. */
			if (malloced)
			{
				free(keyword);
				free(value);
			}
			return false;
		}

		if (malloced)
		{
			free(keyword);
			free(value);
		}

		/* Proceed to next key=value pair, if any */
		params = p;
	}

	return true;
}

/*
 * Connection URI decoder routine
 *
 * If successful, returns the malloc'd decoded string.
 * If not successful, returns NULL and fills errorMessage accordingly.
 *
 * The string is decoded by replacing any percent-encoded tokens with
 * corresponding characters, while preserving any non-encoded characters.  A
 * percent-encoded token is a character triplet: a percent sign, followed by a
 * pair of hexadecimal digits (0-9A-F), where lower- and upper-case letters are
 * treated identically.
 */
static char *
conninfo_uri_decode(const char *str, ExpBuffer errorMessage)
{
	char	   *buf;			/* result */
	char	   *p;				/* output location */
	const char *q = str;		/* input location */

	buf = malloc(strlen(str) + 1);
	if (buf == NULL)
	{
		db_append_error(errorMessage, "out of memory");
		return NULL;
	}
	p = buf;

	/* skip leading whitespaces */
	for (const char *s = q; *s == ' '; s++)
	{
		q++;
		continue;
	}

	for (;;)
	{
		if (*q != '%')
		{
			/* if found a whitespace or NUL, the string ends */
			if (*q == ' ' || *q == '\0')
				goto end;

			/* copy character */
			*(p++) = *(q++);
		}
		else
		{
			int			hi;
			int			lo;
			int			c;

			++q;				/* skip the percent sign itself */

			/*
			 * Possible EOL will be caught by the first call to
			 * get_hexdigit(), so we never dereference an invalid q pointer.
			 */
			if (!(get_hexdigit(*q++, &hi) && get_hexdigit(*q++, &lo)))
			{
				db_append_error(errorMessage,
								   "invalid percent-encoded token: \"%s\"",
								   str);
				free(buf);
				return NULL;
			}

			c = (hi << 4) | lo;
			if (c == 0)
			{
				db_append_error(errorMessage,
								   "forbidden value %%00 in percent-encoded value: \"%s\"",
								   str);
				free(buf);
				return NULL;
			}
			*(p++) = c;
		}
	}

end:

	/* skip trailing whitespaces */
	for (const char *s = q; *s == ' '; s++)
	{
		q++;
		continue;
	}

	/* Not at the end of the string yet?  Fail. */
	if (*q != '\0')
	{
		db_append_error(errorMessage,
						   "unexpected spaces found in \"%s\", use percent-encoded spaces (%%20) instead",
						   str);
		free(buf);
		return NULL;
	}

	/* Copy NUL terminator */
	*p = '\0';

	return buf;
}

/*
 * Convert hexadecimal digit character to its integer value.
 *
 * If successful, returns true and value is filled with digit's base 16 value.
 * If not successful, returns false.
 *
 * Lower- and upper-case letters in the range A-F are treated identically.
 */
static bool
get_hexdigit(char digit, int *value)
{
	if ('0' <= digit && digit <= '9')
		*value = digit - '0';
	else if ('A' <= digit && digit <= 'F')
		*value = digit - 'A' + 10;
	else if ('a' <= digit && digit <= 'f')
		*value = digit - 'a' + 10;
	else
		return false;

	return true;
}

/*
 * Find an option value corresponding to the keyword in the connOptions array.
 *
 * If successful, returns a pointer to the corresponding option's value.
 * If not successful, returns NULL.
 */
static const char *
conninfo_getval(conninfoOption *connOptions,
				const char *keyword)
{
	conninfoOption *option;

	option = conninfo_find(connOptions, keyword);

	return option ? option->val : NULL;
}

/*
 * Store a (new) value for an option corresponding to the keyword in
 * connOptions array.
 *
 * If uri_decode is true, the value is URI-decoded.  The keyword is always
 * assumed to be non URI-encoded.
 *
 * If successful, returns a pointer to the corresponding conninfoOption,
 * which value is replaced with a strdup'd copy of the passed value string.
 * The existing value for the option is free'd before replacing, if any.
 *
 * If not successful, returns NULL and fills errorMessage accordingly.
 * However, if the reason of failure is an invalid keyword being passed and
 * ignoreMissing is true, errorMessage will be left untouched.
 */
static conninfoOption *
conninfo_storeval(conninfoOption *connOptions,
				  const char *keyword, const char *value,
				  ExpBuffer errorMessage, bool ignoreMissing,
				  bool uri_decode)
{
	conninfoOption *option;
	char	   *value_copy;

	/*
	 * For backwards compatibility, requiressl=1 gets translated to
	 * sslmode=require, and requiressl=0 gets translated to sslmode=prefer
	 * (which is the default for sslmode).
	 */
	if (strcmp(keyword, "requiressl") == 0)
	{
		keyword = "sslmode";
		if (value[0] == '1')
			value = "require";
		else
			value = "prefer";
	}

	option = conninfo_find(connOptions, keyword);
	if (option == NULL)
	{
		if (!ignoreMissing)
			db_append_error(errorMessage,
							   "invalid connection option \"%s\"",
							   keyword);
		return NULL;
	}

	if (uri_decode)
	{
		value_copy = conninfo_uri_decode(value, errorMessage);
		if (value_copy == NULL)
			/* conninfo_uri_decode already set an error message */
			return NULL;
	}
	else
	{
		value_copy = strdup(value);
		if (value_copy == NULL)
		{
			db_append_error(errorMessage, "out of memory");
			return NULL;
		}
	}

	free(option->val);
	option->val = value_copy;

	return option;
}

/*
 * Find a conninfoOption option corresponding to the keyword in the
 * connOptions array.
 *
 * If successful, returns a pointer to the corresponding conninfoOption
 * structure.
 * If not successful, returns NULL.
 */
static conninfoOption *
conninfo_find(conninfoOption *connOptions, const char *keyword)
{
	conninfoOption *option;

	for (option = connOptions; option->keyword != NULL; option++)
	{
		if (strcmp(option->keyword, keyword) == 0)
			return option;
	}

	return NULL;
}


/*
 * Return the connection options used for the connection
 */
conninfoOption *
conninfo(Conn *conn)
{
	ExpBufferData errorBuf;
	conninfoOption *connOptions;

	if (conn == NULL)
		return NULL;

	/*
	 * We don't actually report any errors here, but callees want a buffer,
	 * and we prefer not to trash the conn's errorMessage.
	 */
	initExpBuffer(&errorBuf);
	if (ExpBufferDataBroken(errorBuf))
		return NULL;			/* out of memory already :-( */

	connOptions = conninfo_init(&errorBuf);

	if (connOptions != NULL)
	{
		const internalconninfoOption *option;

		for (option = conninfoOptions; option->keyword; option++)
		{
			char	  **connmember;

			if (option->connofs < 0)
				continue;

			connmember = (char **) ((char *) conn + option->connofs);

			if (*connmember)
				conninfo_storeval(connOptions, option->keyword, *connmember,
								  &errorBuf, true, false);
		}
	}

	termExpBuffer(&errorBuf);

	return connOptions;
}


/*
 * Obtain user's home directory, return in given buffer
 *
 * On Unix, this actually returns the user's home directory.  On Windows
 * it returns the PostgreSQL-specific application data folder.
 *
 * This is essentially the same as get_home_path(), but we don't use that
 * because we don't want to pull path.c into libpq (it pollutes application
 * namespace).
 *
 * Returns true on success, false on failure to obtain the directory name.
 *
 * CAUTION: although in most situations failure is unexpected, there are users
 * who like to run applications in a home-directory-less environment.  On
 * failure, you almost certainly DO NOT want to report an error.  Just act as
 * though whatever file you were hoping to find in the home directory isn't
 * there (which it isn't).
 */
bool
GetHomeDirectory(char *buf, int bufsize)
{
#ifndef WIN32
	const char *home;

	home = getenv("HOME");
	if (home && home[0])
	{
		strlcpy(buf, home, bufsize);
		return true;
	}
	else
	{
		struct passwd pwbuf;
		struct passwd *pw;
		char		tmpbuf[1024];
		int			rc;

		rc = getpwuid_r(geteuid(), &pwbuf, tmpbuf, sizeof tmpbuf, &pw);
		if (rc != 0 || !pw)
			return false;
		strlcpy(buf, pw->pw_dir, bufsize);
		return true;
	}
#else
	char		tmppath[MAX_PATH];

	ZeroMemory(tmppath, sizeof(tmppath));
	if (SHGetFolderPath(NULL, CSIDL_APPDATA, NULL, 0, tmppath) != S_OK)
		return false;
	snprintf(buf, bufsize, "%s/postgresql", tmppath);
	return true;
#endif
}

/*
 * Parse and try to interpret "value" as an integer value, and if successful,
 * store it in *result, complaining if there is any trailing garbage or an
 * overflow.  This allows any number of leading and trailing whitespaces.
 */
bool
ParseIntParam(const char *value, int *result, Conn *conn,
				const char *context)
{
	char	   *end;
	long		numval;

	Assert(value != NULL);

	*result = 0;

	/* strtol(3) skips leading whitespaces */
	errno = 0;
	numval = strtol(value, &end, 10);

	/*
	 * If no progress was done during the parsing or an error happened, fail.
	 * This tests properly for overflows of the result.
	 */
	if (value == end || errno != 0 || numval != (int) numval)
		goto error;

	/*
	 * Skip any trailing whitespace; if anything but whitespace remains before
	 * the terminating character, fail
	 */
	while (*end != '\0' && isspace((unsigned char) *end))
		end++;

	if (*end != '\0')
		goto error;

	*result = numval;
	return true;

error:
	db_append_conn_error(conn, "invalid integer value \"%s\" for connection option \"%s\"",
							value, context);
	return false;
}

