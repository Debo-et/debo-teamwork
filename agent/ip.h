/*-------------------------------------------------------------------------
 *
 * ip.h
 *	  Definitions for IPv6-aware network access.
 *
 * These definitions are used by both frontend and backend code.
 *
 *-------------------------------------------------------------------------
 */
#ifndef IP_H
#define IP_H

#include <netdb.h>
#include <sys/socket.h>

#include "comm.h"


extern int	getaddrinfo_all(const char *hostname, const char *servname,
                            const struct addrinfo *hintp,
                            struct addrinfo **result);
extern void freeaddrinfo_all(int hint_ai_family, struct addrinfo *ai);

extern int	getnameinfo_all(const struct sockaddr_storage *addr, int salen,
                            char *node, int nodelen,
                            char *service, int servicelen,
                            int flags);

#endif							/* IP_H */
