/*-------------------------------------------------------------------------
 *
 * expbuffer.h
 *	  Declarations/definitions for "ExpBuffer" functions.
 *
 * ExpBuffer provides an indefinitely-extensible string data type.
 * It can be used to buffer either ordinary C strings (null-terminated text)
 * or arbitrary binary data.  All storage is allocated with malloc().
 *
 * It does rely on vsnprintf(); if configure finds that libc doesn't provide
 * a usable vsnprintf(), then a copy of our own implementation of it will
 * be linked into lib.

 *
 *-------------------------------------------------------------------------
 */
#ifndef EXPBUFFER_H
#define EXPBUFFER_H
#include <stddef.h>
#include <stdarg.h>

/* Define to best printf format archetype, usually gnu_printf if available. */
#define DB_PRINTF_ATTRIBUTE gnu_printf


/* GCC supports format attributes */
#if defined(__GNUC__)
#define db_attribute_format_arg(a) __attribute__((format_arg(a)))
#define db_attribute_printf(f,a) __attribute__((format(DB_PRINTF_ATTRIBUTE, f, a)))
#else
#define db_attribute_format_arg(a)
#define db_attribute_printf(f,a)
#endif

/*-------------------------
 * ExpBufferData holds information about an extensible string.
 *		data	is the current buffer for the string (allocated with malloc).
 *		len		is the current string length.  There is guaranteed to be
 *				a terminating '\0' at data[len], although this is not very
 *				useful when the string holds binary data rather than text.
 *		maxlen	is the allocated size in bytes of 'data', i.e. the maximum
 *				string size (including the terminating '\0' char) that we can
 *				currently store in 'data' without having to reallocate
 *				more space.  We must always have maxlen > len.
 *
 * An exception occurs if we failed to allocate enough memory for the string
 * buffer.  In that case data points to a statically allocated empty string,
 * and len = maxlen = 0.
 *-------------------------
 */
typedef struct ExpBufferData
{
    char	   *data;
    size_t		len;
    size_t		maxlen;
} ExpBufferData;
/* Define to best printf format archetype, usually gnu_printf if available. */
#define PRINTF_ATTRIBUTE gnu_printf

typedef ExpBufferData *ExpBuffer;

/*------------------------
 * Test for a broken (out of memory) ExpBuffer.
 * When a buffer is "broken", all operations except resetting or deleting it
 * are no-ops.
 *------------------------
 */
#define ExpBufferBroken(str)	\
    ((str) == NULL || (str)->maxlen == 0)

/*------------------------
 * Same, but for use when using a static or local ExpBufferData struct.
 * For that, a null-pointer test is useless and may draw compiler warnings.
 *------------------------
 */
#define ExpBufferDataBroken(buf)	\
    ((buf).maxlen == 0)

/*------------------------
 * Initial size of the data buffer in a ExpBuffer.
 * NB: this must be large enough to hold error messages that might
 * be returned by requestCancel().
 *------------------------
 */
#define INITIAL_EXPBUFFER_SIZE	256

/*------------------------
 * There are two ways to create a ExpBuffer object initially:
 *
 * ExpBuffer stringptr = createExpBuffer();
 *		Both the ExpBufferData and the data buffer are malloc'd.
 *
 * ExpBufferData string;
 * initExpBuffer(&string);
 *		The data buffer is malloc'd but the ExpBufferData is presupplied.
 *		This is appropriate if the ExpBufferData is a field of another
 *		struct.
 *-------------------------
 */

/*------------------------
 * createExpBuffer
 * Create an empty 'ExpBufferData' & return a pointer to it.
 */
extern ExpBuffer createExpBuffer(void);

/*------------------------
 * initExpBuffer
 * Initialize a ExpBufferData struct (with previously undefined contents)
 * to describe an empty string.
 */
extern void initExpBuffer(ExpBuffer str);

/*------------------------
 * To destroy a ExpBuffer, use either:
 *
 * destroyExpBuffer(str);
 *		free()s both the data buffer and the ExpBufferData.
 *		This is the inverse of createExpBuffer().
 *
 * term ExpBuffer(str)
 *		free()s the data buffer but not the ExpBufferData itself.
 *		This is the inverse of initExpBuffer().
 *
 * NOTE: some routines build up a string using ExpBuffer, and then
 * release the ExpBufferData but return the data string itself to their
 * caller.  At that point the data string looks like a plain malloc'd
 * string.
 */
extern void destroyExpBuffer(ExpBuffer str);
extern void termExpBuffer(ExpBuffer str);

/*------------------------
 * resetExpBuffer
 *		Reset a ExpBuffer to empty
 *
 * Note: if possible, a "broken" ExpBuffer is returned to normal.
 */
extern void resetExpBuffer(ExpBuffer str);

/*------------------------
 * enlargeExpBuffer
 * Make sure there is enough space for 'needed' more bytes in the buffer
 * ('needed' does not include the terminating null).
 *
 * Returns 1 if OK, 0 if failed to enlarge buffer.  (In the latter case
 * the buffer is left in "broken" state.)
 */
extern int	enlargeExpBuffer(ExpBuffer str, size_t needed);

/*------------------------
 * printfExpBuffer
 * Format text data under the control of fmt (an sprintf-like format string)
 * and insert it into str.  More space is allocated to str if necessary.
 * This is a convenience routine that does the same thing as
 * resetExpBuffer() followed by appendExpBuffer().
 */
extern void printfExpBuffer(ExpBuffer str, const char *fmt,...) db_attribute_printf(2, 3);

/*------------------------
 * appendExpBuffer
 * Format text data under the control of fmt (an sprintf-like format string)
 * and append it to whatever is already in str.  More space is allocated
 * to str if necessary.  This is sort of like a combination of sprintf and
 * strcat.
 */
extern void appendExpBuffer(ExpBuffer str, const char *fmt,...) db_attribute_printf(2, 3);

/*------------------------
 * appendExpBufferVA
 * Attempt to format data and append it to str.  Returns true if done
 * (either successful or hard failure), false if need to retry.
 *
 * Caution: callers must be sure to preserve their entry-time errno
 * when looping, in case the fmt contains "%m".
 */
extern bool appendExpBufferVA(ExpBuffer str, const char *fmt, va_list args) db_attribute_printf(2, 0);

/*------------------------
 * appendExpBufferStr
 * Append the given string to a ExpBuffer, allocating more space
 * if necessary.
 */
extern void appendExpBufferStr(ExpBuffer str, const char *data);

/*------------------------
 * appendExpBufferChar
 * Append a single byte to str.
 * Like appendExpBuffer(str, "%c", ch) but much faster.
 */
extern void appendExpBufferChar(ExpBuffer str, char ch);

/*------------------------
 * appendBinaryExpBuffer
 * Append arbitrary binary data to a ExpBuffer, allocating more space
 * if necessary.
 */
extern void appendBinaryExpBuffer(ExpBuffer str,
                                  const char *data, size_t datalen);

#endif							/* EXPBUFFER_H */
