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
