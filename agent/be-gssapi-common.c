/*-------------------------------------------------------------------------
 *
 * be-gssapi-common.c
 *     Common code for GSSAPI authentication and encryption
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *

 *
 *-------------------------------------------------------------------------
 */

#include <gssapi.h>
#include <gssapi/gssapi.h>
#include <gssapi/gssapi_krb5.h>
#include <string.h>
#include <stdio.h>
#include <stdbool.h>


#define Min(x, y) ((x) < (y) ? (x) : (y))

/*
 * Fetch all errors of a specific type and append to "s" (buffer of size len).
 * If we obtain more than one string, separate them with spaces.
 * Call once for GSS_CODE and once for MECH_CODE.
 */
static void
pg_GSS_error_int(char *s, size_t len, OM_uint32 stat, int type)
{
	gss_buffer_desc gmsg;
	size_t		i = 0;
	OM_uint32	lmin_s,
				msg_ctx = 0;

	do
	{
		if (gss_display_status(&lmin_s, stat, type, GSS_C_NO_OID,
							   &msg_ctx, &gmsg) != GSS_S_COMPLETE)
			break;
		if (i > 0)
		{
			if (i < len)
				s[i] = ' ';
			i++;
		}
		if (i < len)
			memcpy(s + i, gmsg.value, Min(len - i, gmsg.length));
		i += gmsg.length;
		gss_release_buffer(&lmin_s, &gmsg);
	}
	while (msg_ctx);

	/* add nul termination */
	if (i < len)
		s[i] = '\0';
	else
	{
		fprintf(stderr, "incomplete GSS error report\n");
		s[len - 1] = '\0';
	}
}

/*
 * Report the GSSAPI error described by maj_stat/min_stat.
 *
 * errmsg should be an already-translated primary error message.
 * The GSSAPI info is appended as errdetail.
 *
 * The error is always reported with elevel COMMERROR; we daren't try to
 * send it to the client, as that'd likely lead to infinite recursion
 * when elog.c tries to write to the client.
 *
 * To avoid memory allocation, total error size is capped (at 128 bytes for
 * each of major and minor).  No known mechanisms will produce error messages
 * beyond this cap.
 */
void
pg_GSS_error(const char *errmsg,
			 OM_uint32 maj_stat, OM_uint32 min_stat)
{
	char		msg_major[128],
				msg_minor[128];

	/* Fetch major status message */
	pg_GSS_error_int(msg_major, sizeof(msg_major), maj_stat, GSS_C_GSS_CODE);

	/* Fetch mechanism minor status message */
	pg_GSS_error_int(msg_minor, sizeof(msg_minor), min_stat, GSS_C_MECH_CODE);

	/*
	 * errmsg_internal, since translation of the first part must be done
	 * before calling this function anyway.
	 */
	fprintf(stderr, "%s: %s", msg_major, msg_minor);
}

/*
 * Store the credentials passed in into the memory cache for later usage.
 *
 * This allows credentials to be delegated to us for us to use to connect
 * to other systems with, using, e.g. postgres_fdw or dblink.
 */
#define GSS_MEMORY_CACHE "MEMORY:"
void
pg_store_delegated_credential(gss_cred_id_t cred)
{
	OM_uint32	major,
				minor;
	gss_OID_set mech;
	gss_cred_usage_t usage;
	gss_key_value_element_desc cc;
	gss_key_value_set_desc ccset;

	cc.key = "ccache";
	cc.value = GSS_MEMORY_CACHE;
	ccset.count = 1;
	ccset.elements = &cc;

	/* Make the delegated credential only available to current process */
	major = gss_store_cred_into(&minor,
								cred,
								GSS_C_INITIATE, /* credential only used for
												 * starting libpq connection */
								GSS_C_NULL_OID, /* store all */
								true,	/* overwrite */
								true,	/* make default */
								&ccset,
								&mech,
								&usage);

	if (major != GSS_S_COMPLETE)
	{
		pg_GSS_error("gss_store_cred", major, minor);
	}

	/* Credential stored, so we can release our credential handle. */
	major = gss_release_cred(&minor, &cred);
	if (major != GSS_S_COMPLETE)
	{
		pg_GSS_error("gss_release_cred", major, minor);
	}

	/*
	 * Set KRB5CCNAME for this backend, so that later calls to
	 * gss_acquire_cred will find the delegated credentials we stored.
	 */
	setenv("KRB5CCNAME", GSS_MEMORY_CACHE, 1);
}
