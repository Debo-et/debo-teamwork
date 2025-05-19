/*-------------------------------------------------------------------------
 *
 * ifaddr.h
 *	  IP netmask calculations, and enumerating network interfaces.
 *
 *-------------------------------------------------------------------------
 */
#ifndef IFADDR_H
#define IFADDR_H

#include "comm.h"		

typedef void (*DbIfAddrCallback) (struct sockaddr *addr,
								  struct sockaddr *netmask,
								  void *cb_data);

extern int	range_sockaddr(const struct sockaddr_storage *addr,
							  const struct sockaddr_storage *netaddr,
							  const struct sockaddr_storage *netmask);

extern int	sockaddr_cidr_mask(struct sockaddr_storage *mask,
								  char *numbits, int family);

extern int	foreach_ifaddr(DbIfAddrCallback callback, void *cb_data);

#endif							/* IFADDR_H */
