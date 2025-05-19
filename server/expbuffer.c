/*-------------------------------------------------------------------------
 *
 * expbuffer.c
 *
 * ExpBuffer provides an indefinitely-extensible string data type.
 * It can be used to buffer either ordinary C strings (null-terminated text)
 * or arbitrary binary data.  All storage is allocated with malloc().
 *
 * This module is essentially the same as the backend's StringInfo data type,
 * but it is intended for use in frontend  and client applications..
 *
 *-------------------------------------------------------------------------
 */


#include <limits.h>
#include <stdlib.h>
#include <stddef.h>
#include <errno.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>

#include "expbuffer.h"

/*
 * Hints to the compiler about the likelihood of a branch. Both likely() and
 * unlikely() return the boolean value of the contained expression.
 *
 * These should only be used sparingly, in very hot code paths. It's very easy
 * to mis-estimate likelihoods.
 */
#if __GNUC__ >= 3
#define likely(x)       __builtin_expect((x) != 0, 1)
#define unlikely(x) __builtin_expect((x) != 0, 0)
#else
#define likely(x)       ((x) != 0)
#define unlikely(x) ((x) != 0)
#endif



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


/* All "broken" ExpBuffers point to this string. */
static const char oom_buffer[1] = "";

/* Need a char * for unconstify() compatibility */
static const char *const oom_buffer_ptr = oom_buffer;


/*
 * markExpBufferBroken
 *
 * Put a ExpBuffer in "broken" state if it isn't already.
 */
static void
markExpBufferBroken(ExpBuffer str)
{
	if (str->data != oom_buffer)
		free(str->data);

	/*
	 * Casting away const here is a bit ugly, but it seems preferable to not
	 * marking oom_buffer const.  We want to do that to encourage the compiler
	 * to put oom_buffer in read-only storage, so that anyone who tries to
	 * scribble on a broken ExpBuffer will get a failure.
	 */
	str->data = unconstify(char *, oom_buffer_ptr);
	str->len = 0;
	str->maxlen = 0;
}

/*
 * createExpBuffer
 *
 * Create an empty 'ExpBufferData' & return a pointer to it.
 */
ExpBuffer
createExpBuffer(void)
{
	ExpBuffer res;

	res = (ExpBuffer) malloc(sizeof(ExpBufferData));
	if (res != NULL)
		initExpBuffer(res);

	return res;
}

/*
 * initExpBuffer
 *
 * Initialize a ExpBufferData struct (with previously undefined contents)
 * to describe an empty string.
 */
void
initExpBuffer(ExpBuffer str)
{
	str->data = (char *) malloc(INITIAL_EXPBUFFER_SIZE);
	if (str->data == NULL)
	{
		str->data = unconstify(char *, oom_buffer_ptr); /* see comment above */
		str->maxlen = 0;
		str->len = 0;
	}
	else
	{
		str->maxlen = INITIAL_EXPBUFFER_SIZE;
		str->len = 0;
		str->data[0] = '\0';
	}
}

/*
 * destroyExpBuffer(str);
 *
 *		free()s both the data buffer and the ExpBufferData.
 *		This is the inverse of createExpBuffer().
 */
void
destroyExpBuffer(ExpBuffer str)
{
	if (str)
	{
		termExpBuffer(str);
		free(str);
	}
}

/*
 * termExpBuffer(str)
 *		free()s the data buffer but not the ExpBufferData itself.
 *		This is the inverse of initExpBuffer().
 */
void
termExpBuffer(ExpBuffer str)
{
	if (str->data != oom_buffer)
		free(str->data);
	/* just for luck, make the buffer validly empty. */
	str->data = unconstify(char *, oom_buffer_ptr); /* see comment above */
	str->maxlen = 0;
	str->len = 0;
}

/*
 * resetExpBuffer
 *		Reset a ExpBuffer to empty
 *
 * Note: if possible, a "broken" ExpBuffer is returned to normal.
 */
void
resetExpBuffer(ExpBuffer str)
{
	if (str)
	{
		if (str->data != oom_buffer)
		{
			str->len = 0;
			str->data[0] = '\0';
		}
		else
		{
			/* try to reinitialize to valid state */
			initExpBuffer(str);
		}
	}
}

/*
 * enlargeExpBuffer
 * Make sure there is enough space for 'needed' more bytes in the buffer
 * ('needed' does not include the terminating null).
 *
 * Returns 1 if OK, 0 if failed to enlarge buffer.  (In the latter case
 * the buffer is left in "broken" state.)
 */
int
enlargeExpBuffer(ExpBuffer str, size_t needed)
{
	size_t		newlen;
	char	   *newdata;

	if (ExpBufferBroken(str))
		return 0;				/* already failed */

	/*
	 * Guard against ridiculous "needed" values, which can occur if we're fed
	 * bogus data.  Without this, we can get an overflow or infinite loop in
	 * the following.
	 */
	if (needed >= ((size_t) INT_MAX - str->len))
	{
		markExpBufferBroken(str);
		return 0;
	}

	needed += str->len + 1;		/* total space required now */

	/* Because of the above test, we now have needed <= INT_MAX */

	if (needed <= str->maxlen)
		return 1;				/* got enough space already */

	/*
	 * We don't want to allocate just a little more space with each append;
	 * for efficiency, double the buffer size each time it overflows.
	 * Actually, we might need to more than double it if 'needed' is big...
	 */
	newlen = (str->maxlen > 0) ? (2 * str->maxlen) : 64;
	while (needed > newlen)
		newlen = 2 * newlen;

	/*
	 * Clamp to INT_MAX in case we went past it.  Note we are assuming here
	 * that INT_MAX <= UINT_MAX/2, else the above loop could overflow.  We
	 * will still have newlen >= needed.
	 */
	if (newlen > (size_t) INT_MAX)
		newlen = (size_t) INT_MAX;

	newdata = (char *) realloc(str->data, newlen);
	if (newdata != NULL)
	{
		str->data = newdata;
		str->maxlen = newlen;
		return 1;
	}

	markExpBufferBroken(str);
	return 0;
}

/*
 * printfExpBuffer
 * Format text data under the control of fmt (an sprintf-like format string)
 * and insert it into str.  More space is allocated to str if necessary.
 * This is a convenience routine that does the same thing as
 * resetExpBuffer() followed by appendExpBuffer().
 */
void
printfExpBuffer(ExpBuffer str, const char *fmt,...)
{
	int			save_errno = errno;
	va_list		args;
	bool		done;

	resetExpBuffer(str);

	if (ExpBufferBroken(str))
		return;					/* already failed */

	/* Loop in case we have to retry after enlarging the buffer. */
	do
	{
		errno = save_errno;
		va_start(args, fmt);
		done = appendExpBufferVA(str, fmt, args);
		va_end(args);
	} while (!done);
}

/*
 * appendExpBuffer
 *
 * Format text data under the control of fmt (an sprintf-like format string)
 * and append it to whatever is already in str.  More space is allocated
 * to str if necessary.  This is sort of like a combination of sprintf and
 * strcat.
 */
void
appendExpBuffer(ExpBuffer str, const char *fmt,...)
{
	int			save_errno = errno;
	va_list		args;
	bool		done;

	if (ExpBufferBroken(str))
		return;					/* already failed */

	/* Loop in case we have to retry after enlarging the buffer. */
	do
	{
		errno = save_errno;
		va_start(args, fmt);
		done = appendExpBufferVA(str, fmt, args);
		va_end(args);
	} while (!done);
}

/*
 * appendExpBufferVA
 * Shared guts of printfExpBuffer/appendExpBuffer.
 * Attempt to format data and append it to str.  Returns true if done
 * (either successful or hard failure), false if need to retry.
 *
 * Caution: callers must be sure to preserve their entry-time errno
 * when looping, in case the fmt contains "%m".
 */
bool
appendExpBufferVA(ExpBuffer str, const char *fmt, va_list args)
{
	size_t		avail;
	size_t		needed;
	int			nprinted;

	/*
	 * Try to format the given string into the available space; but if there's
	 * hardly any space, don't bother trying, just enlarge the buffer first.
	 */
	if (str->maxlen > str->len + 16)
	{
		avail = str->maxlen - str->len;

		nprinted = vsnprintf(str->data + str->len, avail, fmt, args);

		/*
		 * If vsnprintf reports an error, fail (we assume this means there's
		 * something wrong with the format string).
		 */
		if (unlikely(nprinted < 0))
		{
			markExpBufferBroken(str);
			return true;
		}

		if ((size_t) nprinted < avail)
		{
			/* Success.  Note nprinted does not include trailing null. */
			str->len += nprinted;
			return true;
		}

		/*
		 * We assume a C99-compliant vsnprintf, so believe its estimate of the
		 * required space, and add one for the trailing null.  (If it's wrong,
		 * the logic will still work, but we may loop multiple times.)
		 *
		 * Choke if the required space would exceed INT_MAX, since str->maxlen
		 * can't represent more than that.
		 */
		if (unlikely(nprinted > INT_MAX - 1))
		{
			markExpBufferBroken(str);
			return true;
		}
		needed = nprinted + 1;
	}
	else
	{
		/*
		 * We have to guess at how much to enlarge, since we're skipping the
		 * formatting work.  Fortunately, because of enlargeExpBuffer's
		 * preference for power-of-2 sizes, this number isn't very sensitive;
		 * the net effect is that we'll double the buffer size before trying
		 * to run vsnprintf, which seems sensible.
		 */
		needed = 32;
	}

	/* Increase the buffer size and try again. */
	if (!enlargeExpBuffer(str, needed))
		return true;			/* oops, out of memory */

	return false;
}

/*
 * appendExpBufferStr
 * Append the given string to a ExpBuffer, allocating more space
 * if necessary.
 */
void
appendExpBufferStr(ExpBuffer str, const char *data)
{
	appendBinaryExpBuffer(str, data, strlen(data));
}

/*
 * appendExpBufferChar
 * Append a single byte to str.
 * Like appendExpBuffer(str, "%c", ch) but much faster.
 */
void
appendExpBufferChar(ExpBuffer str, char ch)
{
	/* Make more room if needed */
	if (!enlargeExpBuffer(str, 1))
		return;

	/* OK, append the character */
	str->data[str->len] = ch;
	str->len++;
	str->data[str->len] = '\0';
}

/*
 * appendBinaryExpBuffer
 *
 * Append arbitrary binary data to a ExpBuffer, allocating more space
 * if necessary.
 */
void
appendBinaryExpBuffer(ExpBuffer str, const char *data, size_t datalen)
{
	/* Make more room if needed */
	if (!enlargeExpBuffer(str, datalen))
		return;

	/* OK, append the data */
	memcpy(str->data + str->len, data, datalen);
	str->len += datalen;

	/*
	 * Keep a trailing null in place, even though it's probably useless for
	 * binary data...
	 */
	str->data[str->len] = '\0';
}
