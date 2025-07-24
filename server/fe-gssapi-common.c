/*-------------------------------------------------------------------------
 *
 * fe-gssapi-common.c
 *     The front-end (client) GSSAPI common code
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *-------------------------------------------------------------------------
 */

#include <gssapi.h>
#include <gssapi/gssapi.h>
#include <stdbool.h>
#include <string.h>
#include <stdlib.h>
#include "expbuffer.h"
#include "connect.h"

#define STATUS_OK                               (0)
#define STATUS_ERROR                    (-1)
#define STATUS_EOF                              (-2)


/*
 * Fetch all errors of a specific type and append to "str".
 * Each error string is preceded by a space.
 */
static void
pg_GSS_error_int(ExpBuffer str, OM_uint32 stat, int type)
{
    OM_uint32	lmin_s;
    gss_buffer_desc lmsg;
    OM_uint32	msg_ctx = 0;

    do
    {
        if (gss_display_status(&lmin_s, stat, type, GSS_C_NO_OID,
                               &msg_ctx, &lmsg) != GSS_S_COMPLETE)
            break;
        appendExpBufferChar(str, ' ');
        appendBinaryExpBuffer(str, lmsg.value, lmsg.length);
        gss_release_buffer(&lmin_s, &lmsg);
    } while (msg_ctx);
}

/*
 * GSSAPI errors contain two parts; put both into conn->errorMessage.
 */
void
pg_GSS_error(const char *mprefix, Conn *conn,
             OM_uint32 maj_stat, OM_uint32 min_stat)
{
    appendExpBuffer(&conn->errorMessage, "%s:", mprefix);
    pg_GSS_error_int(&conn->errorMessage, maj_stat, GSS_C_GSS_CODE);
    appendExpBufferChar(&conn->errorMessage, ':');
    pg_GSS_error_int(&conn->errorMessage, min_stat, GSS_C_MECH_CODE);
    appendExpBufferChar(&conn->errorMessage, '\n');
}

/*
 * Check if we can acquire credentials at all (and yield them if so).
 */
bool
pg_GSS_have_cred_cache(gss_cred_id_t *cred_out)
{
    OM_uint32	major,
                minor;
    gss_cred_id_t cred = GSS_C_NO_CREDENTIAL;

    major = gss_acquire_cred(&minor, GSS_C_NO_NAME, 0, GSS_C_NO_OID_SET,
                             GSS_C_INITIATE, &cred, NULL, NULL);
    if (major != GSS_S_COMPLETE)
    {
        *cred_out = NULL;
        return false;
    }
    *cred_out = cred;
    return true;
}

char *
PQhost(const Conn *conn)
{
    if (!conn)
        return NULL;

    if (conn->connhost != NULL)
    {
        /*
         * Return the verbatim host value provided by user, or hostaddr in its
         * lack.
         */
        if (conn->connhost[conn->whichhost].host != NULL &&
            conn->connhost[conn->whichhost].host[0] != '\0')
            return conn->connhost[conn->whichhost].host;
        else if (conn->connhost[conn->whichhost].hostaddr != NULL &&
                 conn->connhost[conn->whichhost].hostaddr[0] != '\0')
            return conn->connhost[conn->whichhost].hostaddr;
    }

    return "";
}

/*
 * Try to load service name for a connection
 */
int
pg_GSS_load_servicename(Conn *conn)
{
    OM_uint32   maj_stat,
                min_stat;
    int         maxlen;
    gss_buffer_desc temp_gbuf;
    char       *host;

    if (conn->gtarg_nam != NULL)
        return STATUS_OK;

    host = PQhost(conn);
    if (!(host && host[0] != '\0'))
    {
        fprintf(stderr, "host name must be specified");
        return STATUS_ERROR;
    }

    // Hardcode service name to "debo"
    const char *service_name = "debo";  // Modification here

    maxlen = strlen(service_name) + strlen(host) + 2;  // Updated
    temp_gbuf.value = (char *) malloc(maxlen);
    if (!temp_gbuf.value)
    {
        fprintf(stderr, "out of memory");
        return STATUS_ERROR;
    }
    snprintf(temp_gbuf.value, maxlen, "%s@%s",
             service_name, host);  // Updated
    temp_gbuf.length = strlen(temp_gbuf.value);

    maj_stat = gss_import_name(&min_stat, &temp_gbuf,
                               GSS_C_NT_HOSTBASED_SERVICE, &conn->gtarg_nam);
    free(temp_gbuf.value);

    if (maj_stat != GSS_S_COMPLETE)
    {
        pg_GSS_error("GSSAPI name import error",  // Removed libpq_gettext for simplicity
                     conn,
                     maj_stat, min_stat);
        return STATUS_ERROR;
    }
    return STATUS_OK;
}
