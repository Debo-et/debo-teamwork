
#include "latch.h"
#include "comm.h"
#include "format.h"
#include <sys/epoll.h>
#include <stdbool.h>
#include <time.h>
#include <errno.h>
#include <unistd.h>



#define Min(x, y) ((x) < (y) ? (x) : (y))

#define INT64CONST(x)  INT64_C(x)
#define PGINVALID_SOCKET (-1)
#define AccessWaitEvent(k_ev) (*((WaitEvent **)(&(k_ev)->udata)))
/*
 * We store interval times as an int64 integer on all platforms, as int64 is
 * cheap to add/subtract, the most common operation for instr_time. The
 * acquisition of time and converting to specific units of time is platform
 * specific.
 *
 * To avoid users of the API relying on the integer representation, we wrap
 * the 64bit integer in a struct.
 */
typedef struct instr_time
{
        int64           ticks;                  /* in platforms specific unit */
} instr_time;

/*
 * The best clockid to use according to the POSIX spec is CLOCK_MONOTONIC,
 * since that will give reliable interval timing even in the face of changes
 * to the system clock.  However, POSIX doesn't require implementations to
 * provide anything except CLOCK_REALTIME, so fall back to that if we don't
 * find CLOCK_MONOTONIC.
 *
 * Also, some implementations have nonstandard clockids with better properties
 * than CLOCK_MONOTONIC.  In particular, as of macOS 10.12, Apple provides
 * CLOCK_MONOTONIC_RAW which is both faster to read and higher resolution than
 * their version of CLOCK_MONOTONIC.
 */
#if defined(__darwin__) && defined(CLOCK_MONOTONIC_RAW)
#define PG_INSTR_CLOCK  CLOCK_MONOTONIC_RAW
#elif defined(CLOCK_MONOTONIC)
#define PG_INSTR_CLOCK  CLOCK_MONOTONIC
#else
#define PG_INSTR_CLOCK  CLOCK_REALTIME
#endif
#define NS_PER_S        INT64CONST(1000000000)

/* helper for INSTR_TIME_SET_CURRENT */
static inline instr_time
pg_clock_gettime_ns(void)
{
        instr_time      now;
        struct timespec tmp;

        clock_gettime(PG_INSTR_CLOCK, &tmp);
        now.ticks = tmp.tv_sec * NS_PER_S + tmp.tv_nsec;

        return now;
}

#define INSTR_TIME_SET_CURRENT(t) \
        ((t) = pg_clock_gettime_ns())
        
#define INSTR_TIME_SET_ZERO(t)  ((t).ticks = 0)
#if defined(__ppc__) || defined(__powerpc__) || defined(__PPC__)
#define pg_memory_barrier_impl()  __asm__ __volatile__ ("sync" : : : "memory")
#elif defined(__x86_64__) || defined(__i386__)
#define pg_memory_barrier_impl()  __asm__ __volatile__ ("mfence" : : : "memory")
#else
#define pg_memory_barrier_impl()  __sync_synchronize()
#endif

#define pg_memory_barrier() pg_memory_barrier_impl()

#define INSTR_TIME_SUBTRACT(x,y) \
        ((x).ticks -= (y).ticks)


#define NS_PER_MS       INT64CONST(1000000)
        
#define INSTR_TIME_GET_NANOSEC(t) \
        ((int64) (t).ticks)


#define INSTR_TIME_GET_MILLISEC(t) \
        ((double) INSTR_TIME_GET_NANOSEC(t) / NS_PER_MS)
        
#define TYPEALIGN(ALIGNVAL,LEN)  \
        (((uintptr_t) (LEN) + ((ALIGNVAL) - 1)) & ~((uintptr_t) ((ALIGNVAL) - 1)))

        
#define MAXIMUM_ALIGNOF 8


#define MAXALIGN(LEN)                   TYPEALIGN(MAXIMUM_ALIGNOF, (LEN))

#define WAIT_USE_EPOLL




/* typedef in latch.h */
struct WaitEventSet
{

	int			nevents;		/* number of registered events */
	int			nevents_space;	/* maximum number of events in this set */

	/*
	 * Array, of nevents_space length, storing the definition of events this
	 * set is waiting for.
	 */
	WaitEvent  *events;

	/*
	 * If WL_LATCH_SET is specified in any wait event, latch is a pointer to
	 * said latch, and latch_pos the offset in the ->events array. This is
	 * useful because we check the state of the latch before performing doing
	 * syscalls related to waiting.
	 */
	Latch	   *latch;
	int			latch_pos;

	/*
	 * WL_EXIT_ON_PM_DEATH is converted to WL_POSTMASTER_DEATH, but this flag
	 * is set so that we'll exit immediately if postmaster death is detected,
	 * instead of returning.
	 */
	bool		exit_on_postmaster_death;

#if defined(WAIT_USE_EPOLL)
	int			epoll_fd;
	/* epoll_wait returns events in a user provided arrays, allocate once */
	struct epoll_event *epoll_ret_events;
#elif defined(WAIT_USE_KQUEUE)
	int			kqueue_fd;
	/* kevent returns events in a user provided arrays, allocate once */
	struct kevent *kqueue_ret_events;
	bool		report_postmaster_not_running;
#elif defined(WAIT_USE_POLL)
	/* poll expects events to be waited on every poll() call, prepare once */
	struct pollfd *pollfds;
#elif defined(WAIT_USE_WIN32)

	/*
	 * Array of windows events. The first element always contains
	 * pgwin32_signal_event, so the remaining elements are offset by one (i.e.
	 * event->pos + 1).
	 */
	HANDLE	   *handles;
#endif
};

#if defined(WAIT_USE_EPOLL)
/*
 * action can be one of EPOLL_CTL_ADD | EPOLL_CTL_MOD | EPOLL_CTL_DEL
 */
static void
WaitEventAdjustEpoll(WaitEventSet *set, WaitEvent *event, int action)
{
	struct epoll_event epoll_ev;
	int			rc;

	/* pointer to our event, returned by epoll_wait */
	epoll_ev.data.ptr = event;
	/* always wait for errors */
	epoll_ev.events = EPOLLERR | EPOLLHUP;

	/* prepare pollfd entry once */
	if (event->events == WL_LATCH_SET)
	{
		Assert(set->latch != NULL);
		epoll_ev.events |= EPOLLIN;
	}
	else if (event->events == WL_POSTMASTER_DEATH)
	{
		epoll_ev.events |= EPOLLIN;
	}
	else
	{
		Assert(event->fd != PGINVALID_SOCKET);
		Assert(event->events & (WL_SOCKET_READABLE |
								WL_SOCKET_WRITEABLE |
								WL_SOCKET_CLOSED));

		if (event->events & WL_SOCKET_READABLE)
			epoll_ev.events |= EPOLLIN;
		if (event->events & WL_SOCKET_WRITEABLE)
			epoll_ev.events |= EPOLLOUT;
		if (event->events & WL_SOCKET_CLOSED)
			epoll_ev.events |= EPOLLRDHUP;
	}

	/*
	 * Even though unused, we also pass epoll_ev as the data argument if
	 * EPOLL_CTL_DEL is passed as action.  There used to be an epoll bug
	 * requiring that, and actually it makes the code simpler...
	 */
	rc = epoll_ctl(set->epoll_fd, action, event->fd, &epoll_ev);

	if (rc < 0)
		  fprintf(stderr, "failed:  epoll_ctl");
}

#endif



/* ---
 * Add an event to the set. Possible events are:
 * - WL_LATCH_SET: Wait for the latch to be set
 * - WL_POSTMASTER_DEATH: Wait for postmaster to die
 * - WL_SOCKET_READABLE: Wait for socket to become readable,
 *	 can be combined in one event with other WL_SOCKET_* events
 * - WL_SOCKET_WRITEABLE: Wait for socket to become writeable,
 *	 can be combined with other WL_SOCKET_* events
 * - WL_SOCKET_CONNECTED: Wait for socket connection to be established,
 *	 can be combined with other WL_SOCKET_* events (on non-Windows
 *	 platforms, this is the same as WL_SOCKET_WRITEABLE)
 * - WL_SOCKET_ACCEPT: Wait for new connection to a server socket,
 *	 can be combined with other WL_SOCKET_* events (on non-Windows
 *	 platforms, this is the same as WL_SOCKET_READABLE)
 * - WL_SOCKET_CLOSED: Wait for socket to be closed by remote peer.
 * - WL_EXIT_ON_PM_DEATH: Exit immediately if the postmaster dies
 *
 * Returns the offset in WaitEventSet->events (starting from 0), which can be
 * used to modify previously added wait events using ModifyWaitEvent().
 *
 * In the WL_LATCH_SET case the latch must be owned by the current process,
 * i.e. it must be a process-local latch initialized with InitLatch, or a
 * shared latch associated with the current process by calling OwnLatch.
 *
 * In the WL_SOCKET_READABLE/WRITEABLE/CONNECTED/ACCEPT cases, EOF and error
 * conditions cause the socket to be reported as readable/writable/connected,
 * so that the caller can deal with the condition.
 *
 * The user_data pointer specified here will be set for the events returned
 * by WaitEventSetWait(), allowing to easily associate additional data with
 * events.
 */
int
AddWaitEventToSet(WaitEventSet *set, uint32 events, int fd, Latch *latch,
				  void *user_data)
{
	WaitEvent  *event;

	/* not enough space */
	Assert(set->nevents < set->nevents_space);

	if (events == WL_EXIT_ON_PM_DEATH)
	{
		events = WL_POSTMASTER_DEATH;
		set->exit_on_postmaster_death = true;
	}


	/* waiting for socket readiness without a socket indicates a bug */
	if (fd == PGINVALID_SOCKET && (events & WL_SOCKET_MASK))
		fprintf(stderr, "cannot wait on socket event without a socket");

	event = &set->events[set->nevents];
	event->pos = set->nevents++;
	event->fd = fd;
	event->events = events;
	event->user_data = user_data;
#ifdef WIN32
	event->reset = false;
#endif

	if (events == WL_LATCH_SET)
	{
		set->latch = latch;
		set->latch_pos = event->pos;
#if defined(WAIT_USE_SELF_PIPE)
		event->fd = selfpipe_readfd;
#elif defined(WAIT_USE_SIGNALFD)
		event->fd = signal_fd;
#else
		event->fd = PGINVALID_SOCKET;
#ifdef WAIT_USE_EPOLL
		return event->pos;
#endif
#endif
	}
	else if (events == WL_POSTMASTER_DEATH)
	{
#ifndef WIN32
		//event->fd = postmaster_alive_fds[POSTMASTER_FD_WATCH];
#endif
	}

	/* perform wait primitive specific initialization, if needed */
#if defined(WAIT_USE_EPOLL)
	WaitEventAdjustEpoll(set, event, EPOLL_CTL_ADD);
#endif
	return event->pos;
}


/*
 * Wait using linux's epoll_wait(2).
 *
 * This is the preferable wait method, as several readiness notifications are
 * delivered, without having to iterate through all of set->events. The return
 * epoll_event struct contain a pointer to our events, making association
 * easy.
 */
static inline int
WaitEventSetWaitBlock(WaitEventSet *set, int cur_timeout,
					  WaitEvent *occurred_events, int nevents)
{
	int			returned_events = 0;
	int			rc;
	WaitEvent  *cur_event;
	struct epoll_event *cur_epoll_event;

	/* Sleep */
	rc = epoll_wait(set->epoll_fd, set->epoll_ret_events,
					Min(nevents, set->nevents_space), cur_timeout);

	/* Check return code */
	if (rc < 0)
	{
		/* EINTR is okay, otherwise complain */
		if (errno != EINTR)
		{
			fprintf(stderr, "failed:  epoll_wait");
		}
		return 0;
	}
	else if (rc == 0)
	{
		/* timeout exceeded */
		return -1;
	}

	/*
	 * At least one event occurred, iterate over the returned epoll events
	 * until they're either all processed, or we've returned all the events
	 * the caller desired.
	 */
	for (cur_epoll_event = set->epoll_ret_events;
		 cur_epoll_event < (set->epoll_ret_events + rc) &&
		 returned_events < nevents;
		 cur_epoll_event++)
	{
		/* epoll's data pointer is set to the associated WaitEvent */
		cur_event = (WaitEvent *) cur_epoll_event->data.ptr;

		occurred_events->pos = cur_event->pos;
		occurred_events->user_data = cur_event->user_data;
		occurred_events->events = 0;

		if (cur_event->events == WL_LATCH_SET &&
			cur_epoll_event->events & (EPOLLIN | EPOLLERR | EPOLLHUP))
		{
			/* Drain the signalfd. */
			//drain();

			if (set->latch && set->latch->is_set)
			{
				occurred_events->fd = PGINVALID_SOCKET;
				occurred_events->events = WL_LATCH_SET;
				occurred_events++;
				returned_events++;
			}
		}
		else if (cur_event->events & (WL_SOCKET_READABLE |
									  WL_SOCKET_WRITEABLE |
									  WL_SOCKET_CLOSED))
		{
			Assert(cur_event->fd != PGINVALID_SOCKET);

			if ((cur_event->events & WL_SOCKET_READABLE) &&
				(cur_epoll_event->events & (EPOLLIN | EPOLLERR | EPOLLHUP)))
			{
				/* data available in socket, or EOF */
				occurred_events->events |= WL_SOCKET_READABLE;
			}

			if ((cur_event->events & WL_SOCKET_WRITEABLE) &&
				(cur_epoll_event->events & (EPOLLOUT | EPOLLERR | EPOLLHUP)))
			{
				/* writable, or EOF */
				occurred_events->events |= WL_SOCKET_WRITEABLE;
			}

			if ((cur_event->events & WL_SOCKET_CLOSED) &&
				(cur_epoll_event->events & (EPOLLRDHUP | EPOLLERR | EPOLLHUP)))
			{
				/* remote peer shut down, or error */
				occurred_events->events |= WL_SOCKET_CLOSED;
			}

			if (occurred_events->events != 0)
			{
				occurred_events->fd = cur_event->fd;
				occurred_events++;
				returned_events++;
			}
		}
	}

	return returned_events;
}



/*
 * Wait for events added to the set to happen, or until the timeout is
 * reached.  At most nevents occurred events are returned.
 *
 * If timeout = -1, block until an event occurs; if 0, check sockets for
 * readiness, but don't block; if > 0, block for at most timeout milliseconds.
 *
 * Returns the number of events occurred, or 0 if the timeout was reached.
 *
 * Returned events will have the fd, pos, user_data fields set to the
 * values associated with the registered event.
 */
int
WaitEventSetWait(WaitEventSet *set, long timeout,
				 WaitEvent *occurred_events, int nevents)
{
	int			returned_events = 0;
	instr_time	start_time;
	instr_time	cur_time;
	long		cur_timeout = -1;

	Assert(nevents > 0);

	/*
	 * Initialize timeout if requested.  We must record the current time so
	 * that we can determine the remaining timeout if interrupted.
	 */
	if (timeout >= 0)
	{
		INSTR_TIME_SET_CURRENT(start_time);
		Assert(timeout >= 0 && timeout <= INT_MAX);
		cur_timeout = timeout;
	}
	else
		INSTR_TIME_SET_ZERO(start_time);


	while (returned_events == 0)
	{
		int			rc;

		/*
		 * Check if the latch is set already. If so, leave the loop
		 * immediately, avoid blocking again. We don't attempt to report any
		 * other events that might also be satisfied.
		 *
		 * If someone sets the latch between this and the
		 * WaitEventSetWaitBlock() below, the setter will write a byte to the
		 * pipe (or signal us and the signal handler will do that), and the
		 * readiness routine will return immediately.
		 *
		 * On unix, If there's a pending byte in the self pipe, we'll notice
		 * whenever blocking. Only clearing the pipe in that case avoids
		 * having to drain it every time WaitLatchOrSocket() is used. Should
		 * the pipe-buffer fill up we're still ok, because the pipe is in
		 * nonblocking mode. It's unlikely for that to happen, because the
		 * self pipe isn't filled unless we're blocking (waiting = true), or
		 * from inside a signal handler in latch_sigurg_handler().
		 *
		 * On windows, we'll also notice if there's a pending event for the
		 * latch when blocking, but there's no danger of anything filling up,
		 * as "Setting an event that is already set has no effect.".
		 *
		 * Note: we assume that the kernel calls involved in latch management
		 * will provide adequate synchronization on machines with weak memory
		 * ordering, so that we cannot miss seeing is_set if a notification
		 * has already been queued.
		 */
		if (set->latch && !set->latch->is_set)
		{
			/* about to sleep on a latch */
			set->latch->maybe_sleeping = true;
			pg_memory_barrier();
			/* and recheck */
		}

		if (set->latch && set->latch->is_set)
		{
			occurred_events->fd = PGINVALID_SOCKET;
			occurred_events->pos = set->latch_pos;
			occurred_events->user_data =
				set->events[set->latch_pos].user_data;
			occurred_events->events = WL_LATCH_SET;
			occurred_events++;
			returned_events++;

			/* could have been set above */
			set->latch->maybe_sleeping = false;

			break;
		}

		/*
		 * Wait for events using the readiness primitive chosen at the top of
		 * this file. If -1 is returned, a timeout has occurred, if 0 we have
		 * to retry, everything >= 1 is the number of returned events.
		 */
		rc = WaitEventSetWaitBlock(set, cur_timeout,
								   occurred_events, nevents);

		if (set->latch)
		{
			Assert(set->latch->maybe_sleeping);
			set->latch->maybe_sleeping = false;
		}

		if (rc == -1)
			break;				/* timeout occurred */
		else
			returned_events = rc;

		/* If we're not done, update cur_timeout for next iteration */
		if (returned_events == 0 && timeout >= 0)
		{
			INSTR_TIME_SET_CURRENT(cur_time);
			INSTR_TIME_SUBTRACT(cur_time, start_time);
			cur_timeout = timeout - (long) INSTR_TIME_GET_MILLISEC(cur_time);
			if (cur_timeout <= 0)
				break;
		}
	}


	return returned_events;
}

/*
 * Clear the latch. Calling WaitLatch after this will sleep, unless
 * the latch is set again before the WaitLatch call.
 */
void
ResetLatch(Latch *latch)
{
	/* Only the owner should reset the latch */
	//Assert(latch->owner_pid == MyProcPid);
	Assert(latch->maybe_sleeping == false);

	latch->is_set = false;

	/*
	 * Ensure that the write to is_set gets flushed to main memory before we
	 * examine any flag variables.  Otherwise a concurrent SetLatch might
	 * falsely conclude that it needn't signal us, even though we have missed
	 * seeing some flag updates that SetLatch was supposed to inform us of.
	 */
	pg_memory_barrier();
}


/*
 * Free a previously created WaitEventSet in a child process after a fork().
 */
void
FreeWaitEventSetAfterFork(WaitEventSet *set)
{
#if defined(WAIT_USE_EPOLL)
	close(set->epoll_fd);
#endif

	free(set);
}

WaitEventSet *
CreateWaitEventSet(int nevents)
{
    WaitEventSet *set;
    char       *data;
    Size        sz = 0;

    // Allocate space for WaitEventSet and events
    sz += MAXALIGN(sizeof(WaitEventSet));
    sz += MAXALIGN(sizeof(WaitEvent) * nevents);

#if defined(WAIT_USE_EPOLL)
    sz += MAXALIGN(sizeof(struct epoll_event) * nevents);
#elif defined(WAIT_USE_KQUEUE)
    sz += MAXALIGN(sizeof(struct kevent) * nevents);
#elif defined(WAIT_USE_POLL)
    sz += MAXALIGN(sizeof(struct pollfd) * nevents);
#elif defined(WAIT_USE_WIN32)
    // Removed +1 (no longer reserving space for a separate latch)
    sz += MAXALIGN(sizeof(HANDLE) * nevents);
#endif

    data = (char *)calloc(1, sz);

    set = (WaitEventSet *) data;
    data += MAXALIGN(sizeof(WaitEventSet));

    set->events = (WaitEvent *) data;
    data += MAXALIGN(sizeof(WaitEvent) * nevents);

#if defined(WAIT_USE_EPOLL)
    set->epoll_ret_events = (struct epoll_event *) data;
    data += MAXALIGN(sizeof(struct epoll_event) * nevents);
#elif defined(WAIT_USE_KQUEUE)
    set->kqueue_ret_events = (struct kevent *) data;
    data += MAXALIGN(sizeof(struct kevent) * nevents);
#elif defined(WAIT_USE_POLL)
    set->pollfds = (struct pollfd *) data;
    data += MAXALIGN(sizeof(struct pollfd) * nevents);
#elif defined(WAIT_USE_WIN32)
    set->handles = (HANDLE) data;
    data += MAXALIGN(sizeof(HANDLE) * nevents);
#endif

    set->latch = NULL;  // Explicitly unset (no latch)
    set->nevents_space = nevents;
    set->exit_on_postmaster_death = false;

#if defined(WAIT_USE_EPOLL)
    set->epoll_fd = epoll_create1(EPOLL_CLOEXEC);
    if (set->epoll_fd < 0) {
        fprintf(stderr, "epoll_create1 failed:");
    }
#elif defined(WAIT_USE_KQUEUE)
    // ... (unchanged kqueue setup)
#elif defined(WAIT_USE_WIN32)
    // Removed pgwin32_signal_event (not needed for socket-only focus)
#endif

    return set;
}


/*
 * Free a previously created WaitEventSet.
 *
 * Note: preferably, this shouldn't have to free any resources that could be
 * inherited across an exec().  If it did, we'd likely leak those resources in
 * many scenarios.  For the epoll case, we ensure that by setting EPOLL_CLOEXEC
 * when the FD is created.  For the Windows case, we assume that the handles
 * involved are non-inheritable.
 */
void
FreeWaitEventSet(WaitEventSet *set)
{

#if defined(WAIT_USE_EPOLL)
	close(set->epoll_fd);
#elif defined(WAIT_USE_KQUEUE)
	close(set->kqueue_fd);
#elif defined(WAIT_USE_WIN32)
	for (WaitEvent *cur_event = set->events;
		 cur_event < (set->events + set->nevents);
		 cur_event++)
	{
		if (cur_event->events & WL_LATCH_SET)
		{
			/* uses the latch's HANDLE */
		}
		else if (cur_event->events & WL_POSTMASTER_DEATH)
		{
			/* uses PostmasterHandle */
		}
		else
		{
			/* Clean up the event object we created for the socket */
			WSAEventSelect(cur_event->fd, NULL, 0);
			WSACloseEvent(set->handles[cur_event->pos + 1]);
		}
	}
#endif

	free(set);
}



/*
 * Sets a latch and wakes up anyone waiting on it.
 *
 * This is cheap if the latch is already set, otherwise not so much.
 *
 * NB: when calling this in a signal handler, be sure to save and restore
 * errno around it.  (That's standard practice in most signal handlers, of
 * course, but we used to omit it in handlers that only set a flag.)
 *
 * NB: this function is called from critical sections and signal handlers so
 * throwing an error is not a good idea.
 */
void
SetLatch(Latch *latch)
{
#ifndef WIN32
	pid_t		owner_pid;
#else
	HANDLE		handle;
#endif

	/*
	 * The memory barrier has to be placed here to ensure that any flag
	 * variables possibly changed by this process have been flushed to main
	 * memory, before we check/set is_set.
	 */
	pg_memory_barrier();

	/* Quick exit if already set */
	if (latch->is_set)
		return;

	latch->is_set = true;

	pg_memory_barrier();
	if (!latch->maybe_sleeping)
		return;

#ifndef WIN32

	/*
	 * See if anyone's waiting for the latch. It can be the current process if
	 * we're in a signal handler. We use the self-pipe or SIGURG to ourselves
	 * to wake up WaitEventSetWaitBlock() without races in that case. If it's
	 * another process, send a signal.
	 *
	 * Fetch owner_pid only once, in case the latch is concurrently getting
	 * owned or disowned. XXX: This assumes that pid_t is atomic, which isn't
	 * guaranteed to be true! In practice, the effective range of pid_t fits
	 * in a 32 bit integer, and so should be atomic. In the worst case, we
	 * might end up signaling the wrong process. Even then, you're very
	 * unlucky if a process with that bogus pid exists and belongs to
	 * Postgres; and PG database processes should handle excess SIGUSR1
	 * interrupts without a problem anyhow.
	 *
	 * Another sort of race condition that's possible here is for a new
	 * process to own the latch immediately after we look, so we don't signal
	 * it. This is okay so long as all callers of ResetLatch/WaitLatch follow
	 * the standard coding convention of waiting at the bottom of their loops,
	 * not the top, so that they'll correctly process latch-setting events
	 * that happen before they enter the loop.
	 */
	owner_pid = latch->owner_pid;
	if (owner_pid == 0)
		return;
	else if (owner_pid == MyProcPid)
		kill(owner_pid, SIGURG);

#else

	/*
	 * See if anyone's waiting for the latch. It can be the current process if
	 * we're in a signal handler.
	 *
	 * Use a local variable here just in case somebody changes the event field
	 * concurrently (which really should not happen).
	 */
	handle = latch->event;
	if (handle)
	{
		SetEvent(handle);

		/*
		 * Note that we silently ignore any errors. We might be in a signal
		 * handler or other critical path where it's not safe to call elog().
		 */
	}
#endif
}


/*
 * Initialize a process-local latch.
 */
void
InitLatch(Latch *latch)
{
        latch->is_set = false;
        latch->maybe_sleeping = false;
        latch->owner_pid = MyProcPid;
        latch->is_shared = false;

#if defined(WAIT_USE_SELF_PIPE)
        /* Assert InitializeLatchSupport has been called in this process */
        Assert(selfpipe_readfd >= 0 && selfpipe_owner_pid == MyProcPid);
#elif defined(WAIT_USE_SIGNALFD)
        /* Assert InitializeLatchSupport has been called in this process */
        Assert(signal_fd >= 0);
#elif defined(WAIT_USE_WIN32)
        latch->event = CreateEvent(NULL, TRUE, FALSE, NULL);
        if (latch->event == NULL)
                elog(ERROR, "CreateEvent failed: error code %lu", GetLastError());
#endif                                                  /* WIN32 */
}

