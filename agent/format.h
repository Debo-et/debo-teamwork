/*
 * Copyright 2025 Surafel Temesgen
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 

/*-------------------------------------------------------------------------
 *
 * format.h
 *		Definitions for formatting and parsing frontend/backend messages
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef FORMAT_H
#define FORMAT_H
#include <stdio.h>
#include <stdlib.h>
#include "stringinfo.h"
typedef uint16_t uint16;
typedef float float4;
typedef double float8;
typedef uint64_t uint64;
typedef uint8_t uint8;
typedef uint32_t uint32;
typedef int64_t int64;
#define FLEXIBLE_ARRAY_MEMBER   /* empty */
struct varlena
{
    char            vl_len_[4];             /* Do not touch this field directly! */
    char            vl_dat[FLEXIBLE_ARRAY_MEMBER];  /* Data content is here */
};


typedef struct varlena bytea;
extern void beginmessage(StringInfo buf, char msgtype);
extern void beginmessage_reuse(StringInfo buf, char msgtype);
extern void endmessage(StringInfo buf);
extern void endmessage_reuse(StringInfo buf);

extern void sendbytes(StringInfo buf, const void *data, int datalen);
extern void sendcountedtext(StringInfo buf, const char *str, int slen);
extern void sendtext(StringInfo buf, const char *str, int slen);
extern void sendstring(StringInfo buf, const char *str);
extern void send_ascii_string(StringInfo buf, const char *str);
extern void sendfloat4(StringInfo buf, float4 f);
extern void sendfloat8(StringInfo buf, float8 f);
#define db_restrict __restrict
#define hton32(x) (x)
#define hton64(x) (x)
#define hton16(x) (x)


/*
 * Append a [u]int8 to a StringInfo buffer, which already has enough space
 * preallocated.
 *
 * The use of db_restrict allows the compiler to optimize the code based on
 * the assumption that buf, buf->len, buf->data and *buf->data don't
 * overlap. Without the annotation buf->len etc cannot be kept in a register
 * over subsequent writeintN calls.
 *
 * The use of StringInfoData * rather than StringInfo is due to MSVC being
 * overly picky and demanding a * before a restrict.
 */
static inline void
writeint8(StringInfoData *db_restrict buf, uint8 i)
{
    uint8		ni = i;

    Assert(buf->len + (int) sizeof(uint8) <= buf->maxlen);
    memcpy((char *db_restrict) (buf->data + buf->len), &ni, sizeof(uint8));
    buf->len += sizeof(uint8);
}

/*
 * Append a [u]int16 to a StringInfo buffer, which already has enough space
 * preallocated.
 */
static inline void
writeint16(StringInfoData *db_restrict buf, uint16 i)
{
    uint16		ni = hton16(i);

    Assert(buf->len + (int) sizeof(uint16) <= buf->maxlen);
    memcpy((char *db_restrict) (buf->data + buf->len), &ni, sizeof(uint16));
    buf->len += sizeof(uint16);
}

/*
 * Append a [u]int32 to a StringInfo buffer, which already has enough space
 * preallocated.
 */
static inline void
writeint32(StringInfoData *db_restrict buf, uint32 i)
{
    uint32		ni = hton32(i);

    Assert(buf->len + (int) sizeof(uint32) <= buf->maxlen);
    memcpy((char *db_restrict) (buf->data + buf->len), &ni, sizeof(uint32));
    buf->len += sizeof(uint32);
}

/*
 * Append a [u]int64 to a StringInfo buffer, which already has enough space
 * preallocated.
 */
static inline void
writeint64(StringInfoData *db_restrict buf, uint64 i)
{
    uint64		ni = hton64(i);

    Assert(buf->len + (int) sizeof(uint64) <= buf->maxlen);
    memcpy((char *db_restrict) (buf->data + buf->len), &ni, sizeof(uint64));
    buf->len += sizeof(uint64);
}

/*
 * Append a null-terminated text string (with conversion) to a buffer with
 * preallocated space.
 *
 * NB: The pre-allocated space needs to be sufficient for the string after
 * converting to client encoding.
 *
 * NB: passed text string must be null-terminated, and so is the data
 * sent to the frontend.
 */
static inline void
writestring(StringInfoData *db_restrict buf, const char *db_restrict str)
{
    int			slen = strlen(str);
    char	   *p;

    Assert(buf->len + slen + 1 <= buf->maxlen);

    memcpy(((char *db_restrict) buf->data + buf->len), p, slen + 1);
    buf->len += slen + 1;

    if (p != str)
        free(p);
}

/* append a binary [u]int8 to a StringInfo buffer */
static inline void
sendint8(StringInfo buf, uint8 i)
{
    enlargeStringInfo(buf, sizeof(uint8));
    writeint8(buf, i);
}

/* append a binary [u]int16 to a StringInfo buffer */
static inline void
sendint16(StringInfo buf, uint16 i)
{
    enlargeStringInfo(buf, sizeof(uint16));
    writeint16(buf, i);
}

/* append a binary [u]int32 to a StringInfo buffer */
static inline void
sendint32(StringInfo buf, uint32 i)
{
    enlargeStringInfo(buf, sizeof(uint32));
    writeint32(buf, i);
}

/* append a binary [u]int64 to a StringInfo buffer */
static inline void
sendint64(StringInfo buf, uint64 i)
{
    enlargeStringInfo(buf, sizeof(uint64));
    writeint64(buf, i);
}

/* append a binary byte to a StringInfo buffer */
static inline void
sendbyte(StringInfo buf, uint8 byt)
{
    sendint8(buf, byt);
}

/*
 * Append a binary integer to a StringInfo buffer
 *
 * This function is deprecated; prefer use of the functions above.
 */
static inline void
sendint(StringInfo buf, uint32 i, int b)
{
    switch (b)
    {
    case 1:
        sendint8(buf, (uint8) i);
        break;
    case 2:
        sendint16(buf, (uint16) i);
        break;
    case 4:
        sendint32(buf, (uint32) i);
        break;
    default:
        fprintf(stderr, "unsupported integer size %d", b);
        break;
    }
}


extern void begintypsend(StringInfo buf);
extern bytea *endtypsend(StringInfo buf);

extern void puttextmessage(char msgtype, const char *str);
extern void putemptymessage(char msgtype);

extern int	getmsgbyte(StringInfo msg);
extern unsigned int getmsgint(StringInfo msg, int b);
extern int64 getmsgint64(StringInfo msg);
extern float4 getmsgfloat4(StringInfo msg);
extern float8 getmsgfloat8(StringInfo msg);
extern const char *getmsgbytes(StringInfo msg, int datalen);
extern void copymsgbytes(StringInfo msg, char *buf, int datalen);
extern char *getmsgtext(StringInfo msg, int rawbytes, int *nbytes);
extern const char *getmsgstring(StringInfo msg);
extern const char *getmsgrawstring(StringInfo msg);
extern void getmsgend(StringInfo msg);

#endif							/* FORMAT_H */
