#include <signal.h>
#include <time.h>

#ifdef WIN32
#include "win32.h"
#else
#include <unistd.h>
#include <sys/select.h>
#include <sys/time.h>
#endif

#ifdef HAVE_POLL_H
#include <poll.h>
#endif

#include "connect.h"
#include <errno.h>
#include <stdlib.h>
#include <string.h>

#define db_ntoh16(x) (x)
#define db_hton32(x) (x)
#define db_ntoh32(x) (x)
#define db_hton16(x) (x)

static int	PutMsgBytes(const void *buf, size_t len, Conn *conn);
static int	SendSome(Conn *conn, int len);
static int	SocketCheck(Conn *conn, int forRead, int forWrite,
						  pg_usec_time_t end_time);
#define DBINVALID_SOCKET (-1)
#define ALL_CONNECTION_FAILURE_ERRNOS \
        EPIPE: \
        case ECONNRESET: \
        case ECONNABORTED: \
        case EHOSTDOWN: \
        case EHOSTUNREACH: \
        case ENETDOWN: \
        case ENETRESET: \
        case ENETUNREACH: \
        case ETIMEDOUT
        
#define DB_STRERROR_R_BUFLEN 256 


ssize_t
secure_raw_read(Conn *conn, void *ptr, size_t len)
{
	ssize_t		n;
	int			result_errno = 0;
	char		sebuf[DB_STRERROR_R_BUFLEN];

	SOCK_ERRNO_SET(0);

	n = recv(conn->sock, ptr, len, 0);

	if (n < 0)
	{
		result_errno = SOCK_ERRNO;

		/* Set error message if appropriate */
		switch (result_errno)
		{
#ifdef EAGAIN
			case EAGAIN:
#endif
#if defined(EWOULDBLOCK) && (!defined(EAGAIN) || (EWOULDBLOCK != EAGAIN))
			case EWOULDBLOCK:
#endif
			case EINTR:
				/* no error message, caller is expected to retry */
				break;

			case EPIPE:
			case ECONNRESET:
				db_append_conn_error(conn, "server closed the connection unexpectedly\n"
										"\tThis probably means the server terminated abnormally\n"
										"\tbefore or while processing the request.");
				break;

			case 0:
				/* If errno didn't get set, treat it as regular EOF */
				n = 0;
				break;

			default:
				db_append_conn_error(conn, "could not receive data from server: %d",
										SOCK_STRERROR(result_errno,
													  sebuf, sizeof(sebuf)));
				break;
		}
	}

	/* ensure we return the intended errno to caller */
	SOCK_ERRNO_SET(result_errno);

	return n;
}

/*
 * Low-level implementation of secure_write.
 *
 * This function reports failure (i.e., returns a negative result) only
 * for retryable errors such as EINTR.  Looping for such cases is to be
 * handled at some outer level, maybe all the way up to the application.
 * For hard failures, we set conn->write_failed and store an error message
 * in conn->write_err_msg, but then claim to have written the data anyway.
 * This is because we don't want to report write failures so long as there
 * is a possibility of reading from the server and getting an error message
 * that could explain why the connection dropped.  Many TCP stacks have
 * race conditions such that a write failure may or may not be reported
 * before all incoming data has been read.
 */
ssize_t
secure_raw_write(Conn *conn, const void *ptr, size_t len)
{
	ssize_t		n;
	int			flags = 0;
	int			result_errno = 0;
	char		msgbuf[1024];

	//DECLARE_SIGPIPE_INFO(spinfo);

	/*
	 * If we already had a write failure, we will never again try to send data
	 * on that connection.  Even if the kernel would let us, we've probably
	 * lost message boundary sync with the server.  conn->write_failed
	 * therefore persists until the connection is reset, and we just discard
	 * all data presented to be written.
	 */
	if (conn->write_failed)
		return len;

#ifdef MSG_NOSIGNAL
	if (conn->sigpipe_flag)
		flags |= MSG_NOSIGNAL;

retry_masked:
#endif							/* MSG_NOSIGNAL */

	//DISABLE_SIGPIPE(conn, spinfo, return -1);

	n = send(conn->sock, ptr, len, flags);

	if (n < 0)
	{
		result_errno = SOCK_ERRNO;

		/*
		 * If we see an EINVAL, it may be because MSG_NOSIGNAL isn't available
		 * on this machine.  So, clear sigpipe_flag so we don't try the flag
		 * again, and retry the send().
		 */
#ifdef MSG_NOSIGNAL
		if (flags != 0 && result_errno == EINVAL)
		{
			conn->sigpipe_flag = false;
			flags = 0;
			goto retry_masked;
		}
#endif							/* MSG_NOSIGNAL */

		/* Set error message if appropriate */
		switch (result_errno)
		{
#ifdef EAGAIN
			case EAGAIN:
#endif
#if defined(EWOULDBLOCK) && (!defined(EAGAIN) || (EWOULDBLOCK != EAGAIN))
			case EWOULDBLOCK:
#endif
			case EINTR:
				/* no error message, caller is expected to retry */
				break;

			case EPIPE:
				/* Set flag for EPIPE */
				//REMEMBER_EPIPE(spinfo, true);

				/* FALL THRU */

			case ECONNRESET:
				conn->write_failed = true;
				/* Store error message in conn->write_err_msg, if possible */
				/* (strdup failure is OK, we'll cope later) */
				snprintf(msgbuf, sizeof(msgbuf),
						 libpq_gettext("server closed the connection unexpectedly\n"
									   "\tThis probably means the server terminated abnormally\n"
									   "\tbefore or while processing the request."));
				/* keep newline out of translated string */
				strlcat(msgbuf, "\n", sizeof(msgbuf));
				conn->write_err_msg = strdup(msgbuf);
				/* Now claim the write succeeded */
				n = len;
				break;

			default:
				conn->write_failed = true;
				/* Store error message in conn->write_err_msg, if possible */
				/* (strdup failure is OK, we'll cope later) */
				snprintf(msgbuf, sizeof(msgbuf),
						 "could not send data to server:");
				/* keep newline out of translated string */
				strlcat(msgbuf, "\n", sizeof(msgbuf));
				conn->write_err_msg = strdup(msgbuf);
				/* Now claim the write succeeded */
				n = len;
				break;
		}
	}

	//RESTORE_SIGPIPE(conn, spinfo);

	/* ensure we return the intended errno to caller */
	SOCK_ERRNO_SET(result_errno);

	return n;
}



/*
 * Getc: get 1 character from the connection
 *
 *	All these routines return 0 on success, EOF on error.
 *	Note that for the Get routines, EOF only means there is not enough
 *	data in the buffer, not that there is necessarily a hard error.
 */
int
Getc(char *result, Conn *conn)
{
	if (conn->inCursor >= conn->inEnd)
		return EOF;

	*result = conn->inBuffer[conn->inCursor++];

	return 0;
}


/*
 * Putc: write 1 char to the current message
 */
int
Putc(char c, Conn *conn)
{
	if (PutMsgBytes(&c, 1, conn))
		return EOF;

	return 0;
}


/*
 * Gets[_append]:
 * get a null-terminated string from the connection,
 * and store it in an expansible ExpBuffer.
 * If we run out of memory, all of the string is still read,
 * but the excess characters are silently discarded.
 */
static int
Gets_internal(ExpBuffer buf, Conn *conn, bool resetbuffer)
{
	/* Copy conn data to locals for faster search loop */
	char	   *inBuffer = conn->inBuffer;
	int			inCursor = conn->inCursor;
	int			inEnd = conn->inEnd;
	int			slen;

	while (inCursor < inEnd && inBuffer[inCursor])
		inCursor++;

	if (inCursor >= inEnd)
		return EOF;

	slen = inCursor - conn->inCursor;

	if (resetbuffer)
		resetExpBuffer(buf);

	appendBinaryExpBuffer(buf, inBuffer + conn->inCursor, slen);

	conn->inCursor = ++inCursor;

	return 0;
}

int
Gets(ExpBuffer buf, Conn *conn)
{
	return Gets_internal(buf, conn, true);
}

int
Gets_append(ExpBuffer buf, Conn *conn)
{
	return Gets_internal(buf, conn, false);
}


/*
 * Puts: write a null-terminated string to the current message
 */
int
Puts(const char *s, Conn *conn)
{
	if (PutMsgBytes(s, strlen(s) + 1, conn))
		return EOF;

	return 0;
}

/*
 * Getnchar:
 *	get a string of exactly len bytes in buffer s, no null termination
 */
int
Getnchar(char *s, size_t len, Conn *conn)
{
	if (len > (size_t) (conn->inEnd - conn->inCursor))
		return EOF;

	memcpy(s, conn->inBuffer + conn->inCursor, len);
	/* no terminating null */

	conn->inCursor += len;

	return 0;
}

/*
 * Skipnchar:
 *	skip over len bytes in input buffer.
 *
 * Note: this is primarily useful for its debug output, which should
 * be exactly the same as for Getnchar.  We assume the data in question
 * will actually be used, but just isn't getting copied anywhere as yet.
 */
int
Skipnchar(size_t len, Conn *conn)
{
	if (len > (size_t) (conn->inEnd - conn->inCursor))
		return EOF;

	conn->inCursor += len;

	return 0;
}

/*
 * Putnchar:
 *	write exactly len bytes to the current message
 */
int
Putnchar(const char *s, size_t len, Conn *conn)
{
	if (PutMsgBytes(s, len, conn))
		return EOF;

	return 0;
}

/*
 * GetInt
 *	read a 2 or 4 byte integer and convert from network byte order
 *	to local byte order
 */
int
GetInt(int *result, size_t bytes, Conn *conn)
{
	uint16		tmp2;
	uint32		tmp4;

	switch (bytes)
	{
		case 2:
			if (conn->inCursor + 2 > conn->inEnd)
				return EOF;
			memcpy(&tmp2, conn->inBuffer + conn->inCursor, 2);
			conn->inCursor += 2;
			*result = (int) db_ntoh16(tmp2);
			break;
		case 4:
			if (conn->inCursor + 4 > conn->inEnd)
				return EOF;
			memcpy(&tmp4, conn->inBuffer + conn->inCursor, 4);
			conn->inCursor += 4;
			*result = (int) db_ntoh32(tmp4);
			break;
		default:
			printf("integer of size not supported by GetInt");
			return EOF;
	}

	return 0;
}

/*
 * PutInt
 * write an integer of 2 or 4 bytes, converting from host byte order
 * to network byte order.
 */
int
PutInt(int value, size_t bytes, Conn *conn)
{
	uint16		tmp2;
	uint32		tmp4;

	switch (bytes)
	{
		case 2:
			tmp2 = db_hton16((uint16) value);
			if (PutMsgBytes((const char *) &tmp2, 2, conn))
				return EOF;
			break;
		case 4:
			tmp4 = db_hton32((uint32) value);
			if (PutMsgBytes((const char *) &tmp4, 4, conn))
				return EOF;
			break;
		default:
			printf("integer of size not supported by PutInt");
			return EOF;
	}

	return 0;
}

/*
 * Make sure conn's output buffer can hold bytes_needed bytes (caller must
 * include already-stored data into the value!)
 *
 * Returns 0 on success, EOF if failed to enlarge buffer
 */
int
CheckOutBufferSpace(size_t bytes_needed, Conn *conn)
{
	int			newsize = conn->outBufSize;
	char	   *newbuf;

	/* Quick exit if we have enough space */
	if (bytes_needed <= (size_t) newsize)
		return 0;

	/*
	 * If we need to enlarge the buffer, we first try to double it in size; if
	 * that doesn't work, enlarge in multiples of 8K.  This avoids thrashing
	 * the malloc pool by repeated small enlargements.
	 *
	 * Note: tests for newsize > 0 are to catch integer overflow.
	 */
	do
	{
		newsize *= 2;
	} while (newsize > 0 && bytes_needed > (size_t) newsize);

	if (newsize > 0 && bytes_needed <= (size_t) newsize)
	{
		newbuf = realloc(conn->outBuffer, newsize);
		if (newbuf)
		{
			/* realloc succeeded */
			conn->outBuffer = newbuf;
			conn->outBufSize = newsize;
			return 0;
		}
	}

	newsize = conn->outBufSize;
	do
	{
		newsize += 8192;
	} while (newsize > 0 && bytes_needed > (size_t) newsize);

	if (newsize > 0 && bytes_needed <= (size_t) newsize)
	{
		newbuf = realloc(conn->outBuffer, newsize);
		if (newbuf)
		{
			/* realloc succeeded */
			conn->outBuffer = newbuf;
			conn->outBufSize = newsize;
			return 0;
		}
	}

	/* realloc failed. Probably out of memory */
	appendExpBufferStr(&conn->errorMessage,
						 "cannot allocate memory for output buffer\n");
	return EOF;
}

/*
 * Make sure conn's input buffer can hold bytes_needed bytes (caller must
 * include already-stored data into the value!)
 *
 * Returns 0 on success, EOF if failed to enlarge buffer
 */
int
CheckInBufferSpace(size_t bytes_needed, Conn *conn)
{
	int			newsize = conn->inBufSize;
	char	   *newbuf;

	/* Quick exit if we have enough space */
	if (bytes_needed <= (size_t) newsize)
		return 0;

	/*
	 * Before concluding that we need to enlarge the buffer, left-justify
	 * whatever is in it and recheck.  The caller's value of bytes_needed
	 * includes any data to the left of inStart, but we can delete that in
	 * preference to enlarging the buffer.  It's slightly ugly to have this
	 * function do this, but it's better than making callers worry about it.
	 */
	bytes_needed -= conn->inStart;

	if (conn->inStart < conn->inEnd)
	{
		if (conn->inStart > 0)
		{
			memmove(conn->inBuffer, conn->inBuffer + conn->inStart,
					conn->inEnd - conn->inStart);
			conn->inEnd -= conn->inStart;
			conn->inCursor -= conn->inStart;
			conn->inStart = 0;
		}
	}
	else
	{
		/* buffer is logically empty, reset it */
		conn->inStart = conn->inCursor = conn->inEnd = 0;
	}

	/* Recheck whether we have enough space */
	if (bytes_needed <= (size_t) newsize)
		return 0;

	/*
	 * If we need to enlarge the buffer, we first try to double it in size; if
	 * that doesn't work, enlarge in multiples of 8K.  This avoids thrashing
	 * the malloc pool by repeated small enlargements.
	 *
	 * Note: tests for newsize > 0 are to catch integer overflow.
	 */
	do
	{
		newsize *= 2;
	} while (newsize > 0 && bytes_needed > (size_t) newsize);

	if (newsize > 0 && bytes_needed <= (size_t) newsize)
	{
		newbuf = realloc(conn->inBuffer, newsize);
		if (newbuf)
		{
			/* realloc succeeded */
			conn->inBuffer = newbuf;
			conn->inBufSize = newsize;
			return 0;
		}
	}

	newsize = conn->inBufSize;
	do
	{
		newsize += 8192;
	} while (newsize > 0 && bytes_needed > (size_t) newsize);

	if (newsize > 0 && bytes_needed <= (size_t) newsize)
	{
		newbuf = realloc(conn->inBuffer, newsize);
		if (newbuf)
		{
			/* realloc succeeded */
			conn->inBuffer = newbuf;
			conn->inBufSize = newsize;
			return 0;
		}
	}

	/* realloc failed. Probably out of memory */
	appendExpBufferStr(&conn->errorMessage,
						 "cannot allocate memory for input buffer\n");
	return EOF;
}

/*
 * ParseDone: after a server-to-client message has successfully
 * been parsed, advance conn->inStart to account for it.
 */
void
ParseDone(Conn *conn, int newInStart)
{
	/* trace server-to-client message */
	//if (conn->Pfdebug)
	//	TraceOutputMessage(conn, conn->inBuffer + conn->inStart, false);

	/* Mark message as done */
	conn->inStart = newInStart;
}

/*
 * PutMsgStart: begin construction of a message to the server
 *
 * msg_type is the message type byte, or 0 for a message without type byte
 * (only startup messages have no type byte)
 *
 * Returns 0 on success, EOF on error
 *
 * The idea here is that we construct the message in conn->outBuffer,
 * beginning just past any data already in outBuffer (ie, at
 * outBuffer+outCount).  We enlarge the buffer as needed to hold the message.
 * When the message is complete, we fill in the length word (if needed) and
 * then advance outCount past the message, making it eligible to send.
 *
 * The state variable conn->outMsgStart points to the incomplete message's
 * length word: it is either outCount or outCount+1 depending on whether
 * there is a type byte.  The state variable conn->outMsgEnd is the end of
 * the data collected so far.
 */
int
PutMsgStart(char msg_type, Conn *conn)
{
	int			lenPos;
	int			endPos;

	/* allow room for message type byte */
	if (msg_type)
		endPos = conn->outCount + 1;
	else
		endPos = conn->outCount;

	/* do we want a length word? */
	lenPos = endPos;
	/* allow room for message length */
	endPos += 4;

	/* make sure there is room for message header */
	if (CheckOutBufferSpace(endPos, conn))
		return EOF;
	/* okay, save the message type byte if any */
	if (msg_type)
		conn->outBuffer[conn->outCount] = msg_type;
	/* set up the message pointers */
	conn->outMsgStart = lenPos;
	conn->outMsgEnd = endPos;
	/* length word, if needed, will be filled in by PutMsgEnd */

	return 0;
}

/*
 * PutMsgBytes: add bytes to a partially-constructed message
 *
 * Returns 0 on success, EOF on error
 */
static int
PutMsgBytes(const void *buf, size_t len, Conn *conn)
{
	/* make sure there is room for it */
	if (CheckOutBufferSpace(conn->outMsgEnd + len, conn))
		return EOF;
	/* okay, save the data */
	memcpy(conn->outBuffer + conn->outMsgEnd, buf, len);
	conn->outMsgEnd += len;
	/* no Pfdebug call here, caller should do it */
	return 0;
}

/*
 * PutMsgEnd: finish constructing a message and possibly send it
 *
 * Returns 0 on success, EOF on error
 *
 * We don't actually send anything here unless we've accumulated at least
 * 8K worth of data (the typical size of a pipe buffer on Unix systems).
 * This avoids sending small partial packets.  The caller must use Flush
 * when it's important to flush all the data out to the server.
 */
int
PutMsgEnd(Conn *conn)
{
	/* Fill in length word if needed */
	if (conn->outMsgStart >= 0)
	{
		uint32		msgLen = conn->outMsgEnd - conn->outMsgStart;

		msgLen = db_hton32(msgLen);
		memcpy(conn->outBuffer + conn->outMsgStart, &msgLen, 4);
	}

	/* trace client-to-server message */
	//if (conn->Pfdebug)
	//{
	//	if (conn->outCount < conn->outMsgStart)
	//		TraceOutputMessage(conn, conn->outBuffer + conn->outCount, true);
	//	else
	//		TraceOutputNoTypeByteMessage(conn,
	//									   conn->outBuffer + conn->outMsgStart);
	//}

	/* Make message eligible to send */
	conn->outCount = conn->outMsgEnd;

	if (conn->outCount >= 8192)
	{
		int			toSend = conn->outCount - (conn->outCount % 8192);

		if (SendSome(conn, toSend) < 0)
			return EOF;
		/* in nonblock mode, don't complain if unable to send it all */
	}

	return 0;
}

/* ----------
 * ReadData: read more data, if any is available
 * Possible return values:
 *	 1: successfully loaded at least one more byte
 *	 0: no data is presently available, but no error detected
 *	-1: error detected (including EOF = connection closure);
 *		conn->errorMessage set
 * NOTE: callers must not assume that pointers or indexes into conn->inBuffer
 * remain valid across this call!
 * ----------
 */
int
ReadData(Conn *conn)
{
	int			someread = 0;
	int			nread;

	if (conn->sock == DBINVALID_SOCKET)
	{
		db_append_conn_error(conn, "connection not open");
		return -1;
	}

	/* Left-justify any data in the buffer to make room */
	if (conn->inStart < conn->inEnd)
	{
		if (conn->inStart > 0)
		{
			memmove(conn->inBuffer, conn->inBuffer + conn->inStart,
					conn->inEnd - conn->inStart);
			conn->inEnd -= conn->inStart;
			conn->inCursor -= conn->inStart;
			conn->inStart = 0;
		}
	}
	else
	{
		/* buffer is logically empty, reset it */
		conn->inStart = conn->inCursor = conn->inEnd = 0;
	}

	/*
	 * If the buffer is fairly full, enlarge it. We need to be able to enlarge
	 * the buffer in case a single message exceeds the initial buffer size. We
	 * enlarge before filling the buffer entirely so as to avoid asking the
	 * kernel for a partial packet. The magic constant here should be large
	 * enough for a TCP packet or Unix pipe bufferload.  8K is the usual pipe
	 * buffer size, so...
	 */
	if (conn->inBufSize - conn->inEnd < 8192)
	{
		if (CheckInBufferSpace(conn->inEnd + (size_t) 8192, conn))
		{
			/*
			 * We don't insist that the enlarge worked, but we need some room
			 */
			if (conn->inBufSize - conn->inEnd < 100)
				return -1;		/* errorMessage already set */
		}
	}

	/* OK, try to read some data */
retry3:
	nread = pg_GSS_read(conn, conn->inBuffer + conn->inEnd,
						  conn->inBufSize - conn->inEnd);
	if (nread < 0)
	{
		switch (SOCK_ERRNO)
		{
			case EINTR:
				goto retry3;

				/* Some systems return EAGAIN/EWOULDBLOCK for no data */
#ifdef EAGAIN
			case EAGAIN:
				return someread;
#endif
#if defined(EWOULDBLOCK) && (!defined(EAGAIN) || (EWOULDBLOCK != EAGAIN))
			case EWOULDBLOCK:
				return someread;
#endif

				/* We might get ECONNRESET etc here if connection failed */
			case ALL_CONNECTION_FAILURE_ERRNOS:
				goto definitelyFailed;

			default:
				/* secure_read set the error message for us */
				return -1;
		}
	}
	if (nread > 0)
	{
		conn->inEnd += nread;

		/*
		 * Hack to deal with the fact that some kernels will only give us back
		 * 1 packet per recv() call, even if we asked for more and there is
		 * more available.  If it looks like we are reading a long message,
		 * loop back to recv() again immediately, until we run out of data or
		 * buffer space.  Without this, the block-and-restart behavior of
		 * libpq's higher levels leads to O(N^2) performance on long messages.
		 *
		 * Since we left-justified the data above, conn->inEnd gives the
		 * amount of data already read in the current message.  We consider
		 * the message "long" once we have acquired 32k ...
		 */
		if (conn->inEnd > 32768 &&
			(conn->inBufSize - conn->inEnd) >= 8192)
		{
			someread = 1;
			goto retry3;
		}
		return 1;
	}

	if (someread)
		return 1;				/* got a zero read after successful tries */


	switch (ReadReady(conn))
	{
		case 0:
			/* definitely no data available */
			return 0;
		case 1:
			/* ready for read */
			break;
		default:
			/* we override ReadReady's message with something more useful */
			goto definitelyEOF;
	}

	/*
	 * Still not sure that it's EOF, because some data could have just
	 * arrived.
	 */
retry4:
	nread = pg_GSS_read(conn, conn->inBuffer + conn->inEnd,
						  conn->inBufSize - conn->inEnd);
	if (nread < 0)
	{
		switch (SOCK_ERRNO)
		{
			case EINTR:
				goto retry4;

				/* Some systems return EAGAIN/EWOULDBLOCK for no data */
#ifdef EAGAIN
			case EAGAIN:
				return 0;
#endif
#if defined(EWOULDBLOCK) && (!defined(EAGAIN) || (EWOULDBLOCK != EAGAIN))
			case EWOULDBLOCK:
				return 0;
#endif

				/* We might get ECONNRESET etc here if connection failed */
			case ALL_CONNECTION_FAILURE_ERRNOS:
				goto definitelyFailed;

			default:
				/* secure_read set the error message for us */
				return -1;
		}
	}
	if (nread > 0)
	{
		conn->inEnd += nread;
		return 1;
	}

	/*
	 * OK, we are getting a zero read even though select() says ready. This
	 * means the connection has been closed.  Cope.
	 */
definitelyEOF:
	db_append_conn_error(conn, "server closed the connection unexpectedly\n"
							"\tThis probably means the server terminated abnormally\n"
							"\tbefore or while processing the request.");

	/* Come here if lower-level code already set a suitable errorMessage */
definitelyFailed:
	conn->status = CONNECTION_BAD;	/* No more connection to backend */
	return -1;
}

/*
 * SendSome: send data waiting in the output buffer.
 *
 * len is how much to try to send (typically equal to outCount, but may
 * be less).
 *
 * Return 0 on success, -1 on failure and 1 when not all data could be sent
 * because the socket would block and the connection is non-blocking.
 *
 * Note that this is also responsible for consuming data from the socket
 * (putting it in conn->inBuffer) in any situation where we can't send
 * all the specified data immediately.
 *
 * If a socket-level write failure occurs, conn->write_failed is set and the
 * error message is saved in conn->write_err_msg, but we clear the output
 * buffer and return zero anyway; this is because callers should soldier on
 * until we have read what we can from the server and checked for an error
 * message.  write_err_msg should be reported only when we are unable to
 * obtain a server error first.  Much of that behavior is implemented at
 * lower levels, but this function deals with some edge cases.
 */
static int
SendSome(Conn *conn, int len)
{
	char	   *ptr = conn->outBuffer;
	int			remaining = conn->outCount;
	int			result = 0;

	/*
	 * If we already had a write failure, we will never again try to send data
	 * on that connection.  Even if the kernel would let us, we've probably
	 * lost message boundary sync with the server.  conn->write_failed
	 * therefore persists until the connection is reset, and we just discard
	 * all data presented to be written.  However, as long as we still have a
	 * valid socket, we should continue to absorb data from the backend, so
	 * that we can collect any final error messages.
	 */
	if (conn->write_failed)
	{
		/* conn->write_err_msg should be set up already */
		conn->outCount = 0;
		/* Absorb input data if any, and detect socket closure */
		if (conn->sock != DBINVALID_SOCKET)
		{
			if (ReadData(conn) < 0)
				return -1;
		}
		return 0;
	}

	if (conn->sock == DBINVALID_SOCKET)
	{
		conn->write_failed = true;
		/* Store error message in conn->write_err_msg, if possible */
		/* (strdup failure is OK, we'll cope later) */
		conn->write_err_msg = strdup(libpq_gettext("connection not open\n"));
		/* Discard queued data; no chance it'll ever be sent */
		conn->outCount = 0;
		return 0;
	}

	/* while there's still data to send */
	while (len > 0)
	{
		int			sent;

#ifndef WIN32
		sent = pg_GSS_write(conn, ptr, len);
#else

		/*
		 * Windows can fail on large sends, per KB article Q201213. The
		 * failure-point appears to be different in different versions of
		 * Windows, but 64k should always be safe.
		 */
		sent = pg_GSS_write(conn, ptr, Min(len, 65536));
#endif

		if (sent < 0)
		{
			/* Anything except EAGAIN/EWOULDBLOCK/EINTR is trouble */
			switch (SOCK_ERRNO)
			{
#ifdef EAGAIN
				case EAGAIN:
					break;
#endif
#if defined(EWOULDBLOCK) && (!defined(EAGAIN) || (EWOULDBLOCK != EAGAIN))
				case EWOULDBLOCK:
					break;
#endif
				case EINTR:
					continue;

				default:
					/* Discard queued data; no chance it'll ever be sent */
					conn->outCount = 0;

					/* Absorb input data if any, and detect socket closure */
					if (conn->sock != DBINVALID_SOCKET)
					{
						if (ReadData(conn) < 0)
							return -1;
					}

					/*
					 * Lower-level code should already have filled
					 * conn->write_err_msg (and set conn->write_failed) or
					 * conn->errorMessage.  In the former case, we pretend
					 * there's no problem; the write_failed condition will be
					 * dealt with later.  Otherwise, report the error now.
					 */
					if (conn->write_failed)
						return 0;
					else
						return -1;
			}
		}
		else
		{
			ptr += sent;
			len -= sent;
			remaining -= sent;
		}

		if (len > 0)
		{
			/*
			 * We didn't send it all, wait till we can send more.
			 *
			 * There are scenarios in which we can't send data because the
			 * communications channel is full, but we cannot expect the server
			 * to clear the channel eventually because it's blocked trying to
			 * send data to us.  (This can happen when we are sending a large
			 * amount of COPY data, and the server has generated lots of
			 * NOTICE responses.)  To avoid a deadlock situation, we must be
			 * prepared to accept and buffer incoming data before we try
			 * again.  Furthermore, it is possible that such incoming data
			 * might not arrive until after we've gone to sleep.  Therefore,
			 * we wait for either read ready or write ready.
			 *
			 * In non-blocking mode, we don't wait here directly, but return 1
			 * to indicate that data is still pending.  The caller should wait
			 * for both read and write ready conditions, and call
			 * consumeInput() on read ready, but just in case it doesn't, we
			 * call ReadData() ourselves before returning.  That's not
			 * enough if the data has not arrived yet, but it's the best we
			 * can do, and works pretty well in practice.  (The documentation
			 * used to say that you only need to wait for write-ready, so
			 * there are still plenty of applications like that out there.)
			 *
			 * Note that errors here don't result in write_failed becoming
			 * set.
			 */
			if (ReadData(conn) < 0)
			{
				result = -1;	/* error message already set up */
				break;
			}

			if (Isnonblocking(conn))
			{
				result = 1;
				break;
			}

			if (Wait(true, true, conn))
			{
				result = -1;
				break;
			}
		}
	}

	/* shift the remaining contents of the buffer */
	if (remaining > 0)
		memmove(conn->outBuffer, ptr, remaining);
	conn->outCount = remaining;

	return result;
}


/*
 * Flush: send any data waiting in the output buffer
 *
 * Return 0 on success, -1 on failure and 1 when not all data could be sent
 * because the socket would block and the connection is non-blocking.
 * (See SendSome comments about how failure should be handled.)
 */
int
Flush(Conn *conn)
{
	if (conn->outCount > 0)
	{
		if (conn->Pfdebug)
			fflush(conn->Pfdebug);

		return SendSome(conn, conn->outCount);
	}

	return 0;
}


/*
 * Wait: wait until we can read or write the connection socket
 *
 * JAB: If SSL enabled and used and forRead, buffered bytes short-circuit the
 * call to select().
 *
 * We also stop waiting and return if the kernel flags an exception condition
 * on the socket.  The actual error condition will be detected and reported
 * when the caller tries to read or write the socket.
 */
int
Wait(int forRead, int forWrite, Conn *conn)
{
	return WaitTimed(forRead, forWrite, conn, -1);
}

/*
 * WaitTimed: wait, but not past end_time.
 *
 * Returns -1 on failure, 0 if the socket is readable/writable, 1 if it timed out.
 *
 * The timeout is specified by end_time, which is the int64 number of
 * microseconds since the Unix epoch (that is, time_t times 1 million).
 * Timeout is infinite if end_time is -1.  Timeout is immediate (no blocking)
 * if end_time is 0 (or indeed, any time before now).
 */
int
WaitTimed(int forRead, int forWrite, Conn *conn, pg_usec_time_t end_time)
{
	int			result;

	result = SocketCheck(conn, forRead, forWrite, end_time);

	if (result < 0)
		return -1;				/* errorMessage is already set */

	if (result == 0)
	{
		db_append_conn_error(conn, "timeout expired");
		return 1;
	}

	return 0;
}

/*
 * ReadReady: is select() saying the file is ready to read?
 * Returns -1 on failure, 0 if not ready, 1 if ready.
 */
int
ReadReady(Conn *conn)
{
	return SocketCheck(conn, 1, 0, 0);
}

/*
 * WriteReady: is select() saying the file is ready to write?
 * Returns -1 on failure, 0 if not ready, 1 if ready.
 */
int
WriteReady(Conn *conn)
{
	return SocketCheck(conn, 0, 1, 0);
}

/*
 * Checks a socket, using poll or select, for data to be read, written,
 * or both.  Returns >0 if one or more conditions are met, 0 if it timed
 * out, -1 if an error occurred.
 */
static int
SocketCheck(Conn *conn, int forRead, int forWrite, pg_usec_time_t end_time)
{
	int			result;

	if (!conn)
		return -1;
	if (conn->sock == DBINVALID_SOCKET)
	{
		db_append_conn_error(conn, "invalid socket");
		return -1;
	}


	/* We will retry as long as we get EINTR */
	do
		result = socketPoll(conn->sock, forRead, forWrite, end_time);
	while (result < 0 && SOCK_ERRNO == EINTR);

	if (result < 0)
	{
		char		sebuf[DB_STRERROR_R_BUFLEN];

		db_append_conn_error(conn, "%s() failed: %d", "select",
								SOCK_STRERROR(SOCK_ERRNO, sebuf, sizeof(sebuf)));
	}

	return result;
}

/*
 * getCurrentTimeUSec: get current time with microsecond precision
 *
 * This provides a platform-independent way of producing a reference
 * value for socketPoll's timeout parameter.
 */
pg_usec_time_t
getCurrentTimeUSec(void)
{
	struct timeval tval;

	gettimeofday(&tval, NULL);
	return (pg_usec_time_t) tval.tv_sec * 1000000 + tval.tv_usec;
}





/*
 * Check a file descriptor for read and/or write data, possibly waiting.
 * If neither forRead nor forWrite are set, immediately return a timeout
 * condition (without waiting).  Return >0 if condition is met, 0
 * if a timeout occurred, -1 if an error or interrupt occurred.
 *
 * The timeout is specified by end_time, which is the int64 number of
 * microseconds since the Unix epoch (that is, time_t times 1 million).
 * Timeout is infinite if end_time is -1.  Timeout is immediate (no blocking)
 * if end_time is 0 (or indeed, any time before now).
 */
int
socketPoll(int sock, int forRead, int forWrite, pg_usec_time_t end_time)
{
	/* We use poll(2) if available, otherwise select(2) */
#ifdef HAVE_POLL
	struct pollfd input_fd;
	int			timeout_ms;

	if (!forRead && !forWrite)
		return 0;

	input_fd.fd = sock;
	input_fd.events = POLLERR;
	input_fd.revents = 0;

	if (forRead)
		input_fd.events |= POLLIN;
	if (forWrite)
		input_fd.events |= POLLOUT;

	/* Compute appropriate timeout interval */
	if (end_time == -1)
		timeout_ms = -1;
	else if (end_time == 0)
		timeout_ms = 0;
	else
	{
		pg_usec_time_t now = getCurrentTimeUSec();

		if (end_time > now)
			timeout_ms = (end_time - now) / 1000;
		else
			timeout_ms = 0;
	}

	return poll(&input_fd, 1, timeout_ms);
#else							/* !HAVE_POLL */

	fd_set		input_mask;
	fd_set		output_mask;
	fd_set		except_mask;
	struct timeval timeout;
	struct timeval *ptr_timeout;

	if (!forRead && !forWrite)
		return 0;

	FD_ZERO(&input_mask);
	FD_ZERO(&output_mask);
	FD_ZERO(&except_mask);
	if (forRead)
		FD_SET(sock, &input_mask);

	if (forWrite)
		FD_SET(sock, &output_mask);
	FD_SET(sock, &except_mask);

	/* Compute appropriate timeout interval */
	if (end_time == -1)
		ptr_timeout = NULL;
	else if (end_time == 0)
	{
		timeout.tv_sec = 0;
		timeout.tv_usec = 0;
		ptr_timeout = &timeout;
	}
	else
	{
		pg_usec_time_t now = getCurrentTimeUSec();

		if (end_time > now)
		{
			timeout.tv_sec = (end_time - now) / 1000000;
			timeout.tv_usec = (end_time - now) % 1000000;
		}
		else
		{
			timeout.tv_sec = 0;
			timeout.tv_usec = 0;
		}
		ptr_timeout = &timeout;
	}

	return select(sock + 1, &input_mask, &output_mask,
				  &except_mask, ptr_timeout);
#endif							/* HAVE_POLL */
}



#ifdef ENABLE_NLS

static void
libpq_binddomain(void)
{
	/*
	 * At least on Windows, there are gettext implementations that fail if
	 * multiple threads call bindtextdomain() concurrently.  Use a mutex and
	 * flag variable to ensure that we call it just once per process.  It is
	 * not known that similar bugs exist on non-Windows platforms, but we
	 * might as well do it the same way everywhere.
	 */
	static volatile bool already_bound = false;
	static pthread_mutex_t binddomain_mutex = PTHREAD_MUTEX_INITIALIZER;

	if (!already_bound)
	{
		/* bindtextdomain() does not preserve errno */
#ifdef WIN32
		int			save_errno = GetLastError();
#else
		int			save_errno = errno;
#endif

		(void) pthread_mutex_lock(&binddomain_mutex);

		if (!already_bound)
		{
			const char *ldir;

			/*
			 * No relocatable lookup here because the calling executable could
			 * be anywhere
			 */
			ldir = getenv("PGLOCALEDIR");
			if (!ldir)
				ldir = LOCALEDIR;
			bindtextdomain(PG_TEXTDOMAIN("libpq"), ldir);
			already_bound = true;
		}

		(void) pthread_mutex_unlock(&binddomain_mutex);

#ifdef WIN32
		SetLastError(save_errno);
#else
		errno = save_errno;
#endif
	}
}

char *
libpq_gettext(const char *msgid)
{
	libpq_binddomain();
	return dgettext(PG_TEXTDOMAIN("libpq"), msgid);
}

char *
libpq_ngettext(const char *msgid, const char *msgid_plural, unsigned long n)
{
	libpq_binddomain();
	return dngettext(PG_TEXTDOMAIN("libpq"), msgid, msgid_plural, n);
}

#endif							/* ENABLE_NLS */


/*
 * Append a formatted string to the given buffer, after translating it.  A
 * newline is automatically appended; the format should not end with a
 * newline.
 */
void
db_append_error(ExpBuffer errorMessage, const char *fmt,...)
{
	int			save_errno = errno;
	bool		done;
	va_list		args;

	Assert(fmt[strlen(fmt) - 1] != '\n');

	if (ExpBufferBroken(errorMessage))
		return;					/* already failed */

	/* Loop in case we have to retry after enlarging the buffer. */
	do
	{
		errno = save_errno;
		va_start(args, fmt);
		done = appendExpBufferVA(errorMessage, libpq_gettext(fmt), args);
		va_end(args);
	} while (!done);

	appendExpBufferChar(errorMessage, '\n');
}

/*
 * Append a formatted string to the error message buffer of the given
 * connection, after translating it.  A newline is automatically appended; the
 * format should not end with a newline.
 */
void
db_append_conn_error(Conn *conn, const char *fmt,...)
{
	int			save_errno = errno;
	bool		done;
	va_list		args;

	Assert(fmt[strlen(fmt) - 1] != '\n');

	if (ExpBufferBroken(&conn->errorMessage))
		return;					/* already failed */

	/* Loop in case we have to retry after enlarging the buffer. */
	do
	{
		errno = save_errno;
		va_start(args, fmt);
		done = appendExpBufferVA(&conn->errorMessage, libpq_gettext(fmt), args);
		va_end(args);
	} while (!done);

	appendExpBufferChar(&conn->errorMessage, '\n');
}
