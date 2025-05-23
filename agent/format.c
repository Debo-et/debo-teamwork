/*-------------------------------------------------------------------------
 *
 * format.c
 *		Routines for formatting and parsing frontend/backend messages
 *
 * Outgoing messages are built up in a StringInfo buffer (which is expansible)
 * and then sent in a single call to putmessage.  This module provides data
 * formatting/conversion routines that are needed to produce valid messages.
 * Note in particular the distinction between "raw data" and "text"; raw data
 * is message protocol characters and binary values that are not subject to
 * character set conversion, while text is converted by character encoding
 * rules.
 *
 * Incoming messages are similarly read into a StringInfo buffer, via
 * getmessage, and then parsed and converted from that using the routines
 * in this module.
 *
 * These same routines support reading and writing of external binary formats
 * (typsend/typreceive routines).  The conversion routines for individual
 * data types are exactly the same, only initialization and completion
 * are different.
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 * Message assembly and output:
 *		beginmessage - initialize StringInfo buffer
 *		sendbyte		- append a raw byte to a StringInfo buffer
 *		sendint		- append a binary integer to a StringInfo buffer
 *		sendint64	- append a binary 8-byte int to a StringInfo buffer
 *		sendfloat4	- append a float4 to a StringInfo buffer
 *		sendfloat8	- append a float8 to a StringInfo buffer
 *		sendbytes	- append raw data to a StringInfo buffer
 *		sendcountedtext - append a counted text string (with character set conversion)
 *		sendtext		- append a text string (with conversion)
 *		sendstring	- append a null-terminated text string (with conversion)
 *		send_ascii_string - append a null-terminated text string (without conversion)
 *		endmessage	- send the completed message to the frontend
 * Note: it is also possible to append data to the StringInfo buffer using
 * the regular StringInfo routines, but this is discouraged since required
 * character set conversion may not occur.
 *
 * typsend support (construct a bytea value containing external binary data):
 *		begintypsend - initialize StringInfo buffer
 *		endtypsend	- return the completed string as a "bytea*"
 *
 * Special-case message output:
 *		puttextmessage - generate a character set-converted message in one step
 *		putemptymessage - convenience routine for message with empty body
 *
 * Message parsing after input:
 *		getmsgbyte	- get a raw byte from a message buffer
 *		getmsgint	- get a binary integer from a message buffer
 *		getmsgint64	- get a binary 8-byte int from a message buffer
 *		getmsgfloat4 - get a float4 from a message buffer
 *		getmsgfloat8 - get a float8 from a message buffer
 *		getmsgbytes	- get raw data from a message buffer
 *		copymsgbytes - copy raw data from a message buffer
 *		getmsgtext	- get a counted text string (with conversion)
 *		getmsgstring - get a null-terminated text string (with conversion)
 *		getmsgrawstring - get a null-terminated text string - NO conversion
 *		getmsgend	- verify message fully consumed
 */
#include <sys/param.h>
#include <errno.h>

#include "connutil.h"
#include "format.h"
#include "stringinfo.h"
#if defined(__cplusplus)
#define unconstify(underlying_type, expr) const_cast<underlying_type>(expr)
#define unvolatize(underlying_type, expr) const_cast<underlying_type>(expr)
#elif defined(HAVE__BUILTIN_TYPES_COMPATIBLE_P)
#define unconstify(underlying_type, expr) \
        (StaticAssertExpr(__builtin_types_compatible_p(__typeof(expr), const underlying_type), \
                                          "wrong cast"), \
         (underlying_type) (expr))
#define unvolatize(underlying_type, expr) \
        (StaticAssertExpr(__builtin_types_compatible_p(__typeof(expr), volatile underlying_type), \
                                          "wrong cast"), \
         (underlying_type) (expr))
#else
#define unconstify(underlying_type, expr) \
        ((underlying_type) (expr))
#define unvolatize(underlying_type, expr) \
        ((underlying_type) (expr))
#endif

#define FLEXIBLE_ARRAY_MEMBER   /* empty */
#define VARHDRSZ                ((int32) sizeof(int32))

/* msb for char */
#define HIGHBIT                                 (0x80)
#define IS_HIGHBIT_SET(ch)              ((unsigned char)(ch) & HIGHBIT)

typedef int32_t int32;


typedef union
{
        struct                                          /* Normal varlena (4-byte length) */
        {
                uint32          va_header;
                char            va_data[FLEXIBLE_ARRAY_MEMBER];
        }                       va_4byte;
        struct                                          /* Compressed-in-line format */
        {
                uint32          va_header;
                uint32          va_tcinfo;      /* Original data size (excludes header) and
                                                                 * compression method; see va_extinfo */
                char            va_data[FLEXIBLE_ARRAY_MEMBER]; /* Compressed data */
        }                       va_compressed;
} varattrib_4b;

#define db_ntoh16(x) (x)

#define SET_VARSIZE_4B(PTR,len) \
        (((varattrib_4b *) (PTR))->va_4byte.va_header = (len) & 0x3FFFFFFF)

#define SET_VARSIZE(PTR, len)                           SET_VARSIZE_4B(PTR, len)


/* --------------------------------
 *		beginmessage		- initialize for sending a message
 * --------------------------------
 */
void
beginmessage(StringInfo buf, char msgtype)
{
	initStringInfo(buf);

	/*
	 * We stash the message type into the buffer's cursor field, expecting
	 * that the sendXXX routines won't touch it.  We could alternatively
	 * make it the first byte of the buffer contents, but this seems easier.
	 */
	buf->cursor = msgtype;
}

/* --------------------------------

 *		beginmessage_reuse - initialize for sending a message, reuse buffer
 *
 * This requires the buffer to be allocated in a sufficiently long-lived
 * memory context.
 * --------------------------------
 */
void
beginmessage_reuse(StringInfo buf, char msgtype)
{
	resetStringInfo(buf);

	/*
	 * We stash the message type into the buffer's cursor field, expecting
	 * that the sendXXX routines won't touch it.  We could alternatively
	 * make it the first byte of the buffer contents, but this seems easier.
	 */
	buf->cursor = msgtype;
}

/* --------------------------------
 *		sendbytes	- append raw data to a StringInfo buffer
 * --------------------------------
 */
void
sendbytes(StringInfo buf, const void *data, int datalen)
{
	/* use variant that maintains a trailing null-byte, out of caution */
	appendBinaryStringInfo(buf, data, datalen);
}

/* --------------------------------
 *		sendcountedtext - append a counted text string (with character set conversion)
 *
 * The data sent to the frontend by this routine is a 4-byte count field
 * followed by the string.  The count does not include itself
 * .  The passed text string need not be null-terminated,
 * and the data sent to the frontend isn't either.
 * --------------------------------
 */
void
sendcountedtext(StringInfo buf, const char *str, int slen)
{
    sendint32(buf, slen);
    appendBinaryStringInfoNT(buf, str, slen);
}

/* --------------------------------
 *		sendtext		- append a text string (with conversion)
 *
 * The passed text string need not be null-terminated, and the data sent
 * to the frontend isn't either.  Note that this is not actually useful
 * for direct frontend transmissions, since there'd be no way for the
 * frontend to determine the string length.  But it is useful for binary
 * format conversions.
 * --------------------------------
 */
void
sendtext(StringInfo buf, const char *str, int slen)
{
    appendBinaryStringInfo(buf, str, slen);
}

/* --------------------------------
 *		sendstring	- append a null-terminated text string (with conversion)
 *
 * NB: passed text string must be null-terminated, and so is the data
 * sent to the frontend.
 * --------------------------------
 */
void
sendstring(StringInfo buf, const char *str)
{
	int			slen = strlen(str);
	appendBinaryStringInfoNT(buf, str, slen + 1);
}

/* --------------------------------
 *		send_ascii_string	- append a null-terminated text string (without conversion)
 *
 * This function intentionally bypasses encoding conversion, instead just
 * silently replacing any non-7-bit-ASCII characters with question marks.
 * It is used only when we are having trouble sending an error message to
 * the client with normal localization and encoding conversion.  The caller
 * should already have taken measures to ensure the string is just ASCII;
 * the extra work here is just to make certain we don't send a badly encoded
 * string to the client (which might or might not be robust about that).
 *
 * NB: passed text string must be null-terminated, and so is the data
 * sent to the frontend.
 * --------------------------------
 */
void
send_ascii_string(StringInfo buf, const char *str)
{
	while (*str)
	{
		char		ch = *str++;

		if (IS_HIGHBIT_SET(ch))
			ch = '?';
		appendStringInfoCharMacro(buf, ch);
	}
	appendStringInfoChar(buf, '\0');
}

/* --------------------------------
 *		sendfloat4	- append a float4 to a StringInfo buffer
 *
 * The point of this routine is to localize knowledge of the external binary
 * representation of float4, which is a component of several datatypes.
 *
 * We currently assume that float4 should be byte-swapped in the same way
 * as int4.  This rule is not perfect but it gives us portability across
 * most IEEE-float-using architectures.
 * --------------------------------
 */
void
sendfloat4(StringInfo buf, float4 f)
{
	union
	{
		float4		f;
		uint32		i;
	}			swap;

	swap.f = f;
	sendint32(buf, swap.i);
}

/* --------------------------------
 *		sendfloat8	- append a float8 to a StringInfo buffer
 *
 * The point of this routine is to localize knowledge of the external binary
 * representation of float8, which is a component of several datatypes.
 *
 * We currently assume that float8 should be byte-swapped in the same way
 * as int8.  This rule is not perfect but it gives us portability across
 * most IEEE-float-using architectures.
 * --------------------------------
 */
void
sendfloat8(StringInfo buf, float8 f)
{
	union
	{
		float8		f;
		int64		i;
	}			swap;

	swap.f = f;
	sendint64(buf, swap.i);
}

/* --------------------------------
 *		endmessage	- send the completed message to the frontend
 *
 * The data buffer is free()d, but if the StringInfo was allocated with
 * makeStringInfo then the caller must still free it.
 * --------------------------------
 */
void
endmessage(StringInfo buf)
{
	/* msgtype was saved in cursor field */
	(void) putmessage(buf->cursor, buf->data, buf->len);
	/* no need to complain about any failure, since pqcomm.c already did */
	free(buf->data);
	buf->data = NULL;
}

/* --------------------------------
 *		endmessage_reuse	- send the completed message to the frontend
 *
 * The data buffer is *not* freed, allowing to reuse the buffer with
 * beginmessage_reuse.
 --------------------------------
 */

void
endmessage_reuse(StringInfo buf)
{
	/* msgtype was saved in cursor field */
	(void) putmessage(buf->cursor, buf->data, buf->len);
}


/* --------------------------------
 *		begintypsend		- initialize for constructing a bytea result
 * --------------------------------
 */
void
begintypsend(StringInfo buf)
{
	initStringInfo(buf);
	/* Reserve four bytes for the bytea length word */
	appendStringInfoCharMacro(buf, '\0');
	appendStringInfoCharMacro(buf, '\0');
	appendStringInfoCharMacro(buf, '\0');
	appendStringInfoCharMacro(buf, '\0');
}

/* --------------------------------
 *		endtypsend	- finish constructing a bytea result
 *
 * The data buffer is returned as the alloc'd bytea value.  (We expect
 * that it will be suitably aligned for this because it has been malloc'd.)
 * We assume the StringInfoData is just a local variable in the caller and
 * need not be free'd.
 * --------------------------------
 */
bytea *
endtypsend(StringInfo buf)
{
	bytea	   *result = (bytea *) buf->data;

	/* Insert correct length into bytea length word */
	Assert(buf->len >= VARHDRSZ);
	SET_VARSIZE(result, buf->len);

	return result;
}


/* --------------------------------
 *		puttextmessage - generate a character set-converted message in one step
 *
 *		This is the same as the comm.c routine putmessage, except that
 *		the message body is a null-terminated string to which encoding
 *		conversion applies.
 * --------------------------------
 */
void
puttextmessage(char msgtype, const char *str)
{
	int			slen = strlen(str);

	(void) putmessage(msgtype, str, slen + 1);
}


/* --------------------------------
 *		putemptymessage - convenience routine for message with empty body
 * --------------------------------
 */
void
putemptymessage(char msgtype)
{
	(void) putmessage(msgtype, NULL, 0);
}


/* --------------------------------
 *		getmsgbyte	- get a raw byte from a message buffer
 * --------------------------------
 */
int
getmsgbyte(StringInfo msg)
{
	if (msg->cursor >= msg->len)
		  printf("no data left in message");
	return (unsigned char) msg->data[msg->cursor++];
}

#define pg_ntoh32(x) (x)
/* --------------------------------
 *		getmsgint	- get a binary integer from a message buffer
 *
 *		Values are treated as unsigned.
 * --------------------------------
 */
unsigned int
getmsgint(StringInfo msg, int b)
{
	unsigned int result;
	unsigned char n8;
	uint16		n16;
	uint32		n32;

	switch (b)
	{
		case 1:
			copymsgbytes(msg, (char *) &n8, 1);
			result = n8;
			break;
		case 2:
			copymsgbytes(msg, (char *) &n16, 2);
			result = db_ntoh16(n16);
			break;
		case 4:
			copymsgbytes(msg, (char *) &n32, 4);
			result = pg_ntoh32(n32);
			break;
		default:
			fprintf(stderr, "unsupported integer size %d", b);
			result = 0;			/* keep compiler quiet */
			break;
	}
	return result;
}

#define db_ntoh64(x) (x)
/* --------------------------------
 *		getmsgint64	- get a binary 8-byte int from a message buffer
 *
 * It is tempting to merge this with getmsgint, but we'd have to make the
 * result int64 for all data widths --- that could be a big performance
 * hit on machines where int64 isn't efficient.
 * --------------------------------
 */
int64
getmsgint64(StringInfo msg)
{
	uint64		n64;

	copymsgbytes(msg, (char *) &n64, sizeof(n64));

	return db_ntoh64(n64);
}

/* --------------------------------
 *		getmsgfloat4 - get a float4 from a message buffer
 *
 * See notes for sendfloat4.
 * --------------------------------
 */
float4
getmsgfloat4(StringInfo msg)
{
	union
	{
		float4		f;
		uint32		i;
	}			swap;

	swap.i = getmsgint(msg, 4);
	return swap.f;
}

/* --------------------------------
 *		getmsgfloat8 - get a float8 from a message buffer
 *
 * See notes for sendfloat8.
 * --------------------------------
 */
float8
getmsgfloat8(StringInfo msg)
{
	union
	{
		float8		f;
		int64		i;
	}			swap;

	swap.i = getmsgint64(msg);
	return swap.f;
}

/* --------------------------------
 *		getmsgbytes	- get raw data from a message buffer
 *
 *		Returns a pointer directly into the message buffer; note this
 *		may not have any particular alignment.
 * --------------------------------
 */
const char *
getmsgbytes(StringInfo msg, int datalen)
{
	const char *result;

	if (datalen < 0 || datalen > (msg->len - msg->cursor))
		  printf("insufficient data left in message");
	result = &msg->data[msg->cursor];
	msg->cursor += datalen;
	return result;
}

/* --------------------------------
 *		copymsgbytes - copy raw data from a message buffer
 *
 *		Same as above, except data is copied to caller's buffer.
 * --------------------------------
 */
void
copymsgbytes(StringInfo msg, char *buf, int datalen)
{
	if (datalen < 0 || datalen > (msg->len - msg->cursor))
		printf("insufficient data left in message");
	memcpy(buf, &msg->data[msg->cursor], datalen);
	msg->cursor += datalen;
}

/* --------------------------------
 *		getmsgtext	- get a counted text string (with conversion)
 *
 *		Always returns a pointer to a freshly malloc'd result.
 *		The result has a trailing null, *and* we return its strlen in *nbytes.
 * --------------------------------
 */
char *
getmsgtext(StringInfo msg, int rawbytes, int *nbytes)
{
	char	   *str;
	char	   *p;

	if (rawbytes < 0 || rawbytes > (msg->len - msg->cursor))
		    printf("insufficient data left in message");
	str = &msg->data[msg->cursor];
	msg->cursor += rawbytes;
	
	p = (char *) malloc(rawbytes + 1);
	memcpy(p, str, rawbytes);
	p[rawbytes] = '\0';
	*nbytes = rawbytes;
	return p;
}

/* --------------------------------
 *		getmsgstring - get a null-terminated text string (with conversion)
 *
 *		May return a pointer directly into the message buffer, or a pointer
 *		to a malloc'd conversion result.
 * --------------------------------
 */
const char *
getmsgstring(StringInfo msg)
{
	char	   *str;
	int			slen;

	str = &msg->data[msg->cursor];

	/*
	 * It's safe to use strlen() here because a StringInfo is guaranteed to
	 * have a trailing null byte.  But check we found a null inside the
	 * message.
	 */
	slen = strlen(str);
	if (msg->cursor + slen >= msg->len)
		    printf("invalid string in message");
	msg->cursor += slen + 1;

	return unconstify(char *, str);
}

/* --------------------------------
 *		getmsgrawstring - get a null-terminated text string - NO conversion
 *
 *		Returns a pointer directly into the message buffer.
 * --------------------------------
 */
const char *
getmsgrawstring(StringInfo msg)
{
	char	   *str;
	int			slen;

	str = &msg->data[msg->cursor];

	/*
	 * It's safe to use strlen() here because a StringInfo is guaranteed to
	 * have a trailing null byte.  But check we found a null inside the
	 * message.
	 */
	slen = strlen(str);
	if (msg->cursor + slen >= msg->len)
		  printf("invalid string in message");
	msg->cursor += slen + 1;

	return str;
}

/* --------------------------------
 *		getmsgend	- verify message fully consumed
 * --------------------------------
 */
void
getmsgend(StringInfo msg)
{
	if (msg->cursor != msg->len)
		   printf("invalid message format");
}
