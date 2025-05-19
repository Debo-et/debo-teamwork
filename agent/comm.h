/*-------------------------------------------------------------------------
 *
 * comm.h
 *		Definitions common to frontends and backends.
 */
#ifndef COMM_H
#define COMM_H

#include <sys/socket.h>
#include <sys/un.h>
#include <netdb.h>
#include <netinet/in.h>
#include <stdbool.h>

typedef size_t Size;
#define LONG_ALIGN_MASK (sizeof(long) - 1)
#define MEMSET_LOOP_LIMIT 1024

/*
 * pg_noreturn corresponds to the C11 noreturn/_Noreturn function specifier.
 * We can't use the standard name "noreturn" because some third-party code
 * uses __attribute__((noreturn)) in headers, which would get confused if
 * "noreturn" is defined to "_Noreturn", as is done by <stdnoreturn.h>.
 *
 * In a declaration, function specifiers go before the function name.  The
 * common style is to put them before the return type.  (The MSVC fallback has
 * the same requirement.  The GCC fallback is more flexible.)
 */
#if defined(__STDC_VERSION__) && __STDC_VERSION__ >= 201112L
#define db_noreturn _Noreturn
#elif defined(__GNUC__) || defined(__SUNPRO_C)
#define db_noreturn __attribute__((noreturn))
#elif defined(_MSC_VER)
#define db_noreturn __declspec(noreturn)
#else
#define db_noreturn
#endif


#define MemSet(start, val, len) \
        do \
        { \
                /* must be void* because we don't know if it is integer aligned yet */ \
                void   *_vstart = (void *) (start); \
                int             _val = (val); \
                Size    _len = (len); \
\
                if ((((uintptr_t) _vstart) & LONG_ALIGN_MASK) == 0 && \
                        (_len & LONG_ALIGN_MASK) == 0 && \
                        _val == 0 && \
                        _len <= MEMSET_LOOP_LIMIT && \
                        /* \
                         *      If MEMSET_LOOP_LIMIT == 0, optimizer should find \
                         *      the whole "if" false at compile time. \
                         */ \
                        MEMSET_LOOP_LIMIT != 0) \
                { \
                        long *_start = (long *) _vstart; \
                        long *_stop = (long *) ((char *) _start + _len); \
                        while (_start < _stop) \
                                *_start++ = 0; \
                } \
                else \
                        memset(_vstart, _val, _len); \
        } while (0)


/* ----------------------------------------------------------------
 *				Section 6:	assertions
 * ----------------------------------------------------------------
 */

/*
 * USE_ASSERT_CHECKING, if defined, turns on all the assertions.
 * - plai  9/5/90
 *
 * It should _NOT_ be defined in releases or in benchmark copies
 */

/*
 * Assert() can be used in both frontend and backend code. In frontend code it
 * just calls the standard assert, if it's available. If use of assertions is
 * not configured, it does nothing.
 */
#ifndef USE_ASSERT_CHECKING

#define Assert(condition)	((void)true)
#define AssertMacro(condition)	((void)true)

#elif defined(FRONTEND)

#include <assert.h>
#define Assert(p) assert(p)
#define AssertMacro(p)	((void) assert(p))

#else							/* USE_ASSERT_CHECKING && !FRONTEND */

/*
 * Assert
 *		Generates a fatal exception if the given condition is false.
 */
#define Assert(condition) \
	do { \
		if (!(condition)) \
			ExceptionalCondition(#condition, __FILE__, __LINE__); \
	} while (0)

/*
 * AssertMacro is the same as Assert but it's suitable for use in
 * expression-like macros, for example:
 *
 *		#define foo(x) (AssertMacro(x != 0), bar(x))
 */
#define AssertMacro(condition) \
	((void) ((condition) || \
			 (ExceptionalCondition(#condition, __FILE__, __LINE__), 0)))

#endif							/* USE_ASSERT_CHECKING && !FRONTEND */

/*
 * Check that `ptr' is `bndr' aligned.
 */
#define AssertPointerAlignment(ptr, bndr) \
	Assert(TYPEALIGN(bndr, (uintptr_t)(ptr)) == (uintptr_t)(ptr))

/*
 * ExceptionalCondition is compiled into the backend whether or not
 * USE_ASSERT_CHECKING is defined, so as to support use of extensions
 * that are built with that #define with a backend that isn't.  Hence,
 * we should declare it as long as !FRONTEND.
 */
#ifndef FRONTEND
db_noreturn extern void ExceptionalCondition(const char *conditionName,
											 const char *fileName, int lineNumber);
#endif

/*
 * Macros to support compile-time assertion checks.
 *
 * If the "condition" (a compile-time-constant expression) evaluates to false,
 * throw a compile error using the "errmessage" (a string literal).
 *
 * C11 has _Static_assert(), and most C99 compilers already support that.  For
 * portability, we wrap it into StaticAssertDecl().  _Static_assert() is a
 * "declaration", and so it must be placed where for example a variable
 * declaration would be valid.  As long as we compile with
 * -Wno-declaration-after-statement, that also means it cannot be placed after
 * statements in a function.  Macros StaticAssertStmt() and StaticAssertExpr()
 * make it safe to use as a statement or in an expression, respectively.
 *
 * For compilers without _Static_assert(), we fall back on a kluge that
 * assumes the compiler will complain about a negative width for a struct
 * bit-field.  This will not include a helpful error message, but it beats not
 * getting an error at all.
 */
#ifndef __cplusplus
#ifdef HAVE__STATIC_ASSERT
#define StaticAssertDecl(condition, errmessage) \
	_Static_assert(condition, errmessage)
#define StaticAssertStmt(condition, errmessage) \
	do { _Static_assert(condition, errmessage); } while(0)
#define StaticAssertExpr(condition, errmessage) \
	((void) ({ StaticAssertStmt(condition, errmessage); true; }))
#else							/* !HAVE__STATIC_ASSERT */
#define StaticAssertDecl(condition, errmessage) \
	extern void static_assert_func(int static_assert_failure[(condition) ? 1 : -1])
#define StaticAssertStmt(condition, errmessage) \
	((void) sizeof(struct { int static_assert_failure : (condition) ? 1 : -1; }))
#define StaticAssertExpr(condition, errmessage) \
	StaticAssertStmt(condition, errmessage)
#endif							/* HAVE__STATIC_ASSERT */
#else							/* C++ */
#if defined(__cpp_static_assert) && __cpp_static_assert >= 200410
#define StaticAssertDecl(condition, errmessage) \
	static_assert(condition, errmessage)
#define StaticAssertStmt(condition, errmessage) \
	static_assert(condition, errmessage)
#define StaticAssertExpr(condition, errmessage) \
	({ static_assert(condition, errmessage); })
#else							/* !__cpp_static_assert */
#define StaticAssertDecl(condition, errmessage) \
	extern void static_assert_func(int static_assert_failure[(condition) ? 1 : -1])
#define StaticAssertStmt(condition, errmessage) \
	do { struct static_assert_struct { int static_assert_failure : (condition) ? 1 : -1; }; } while(0)
#define StaticAssertExpr(condition, errmessage) \
	((void) ({ StaticAssertStmt(condition, errmessage); }))
#endif							/* __cpp_static_assert */
#endif							/* C++ */


/*
 * Compile-time checks that a variable (or expression) has the specified type.
 *
 * AssertVariableIsOfType() can be used as a statement.
 * AssertVariableIsOfTypeMacro() is intended for use in macros, eg
 *		#define foo(x) (AssertVariableIsOfTypeMacro(x, int), bar(x))
 *
 * If we don't have __builtin_types_compatible_p, we can still assert that
 * the types have the same size.  This is far from ideal (especially on 32-bit
 * platforms) but it provides at least some coverage.
 */
#ifdef HAVE__BUILTIN_TYPES_COMPATIBLE_P
#define AssertVariableIsOfType(varname, typename) \
	StaticAssertStmt(__builtin_types_compatible_p(__typeof__(varname), typename), \
	CppAsString(varname) " does not have type " CppAsString(typename))
#define AssertVariableIsOfTypeMacro(varname, typename) \
	(StaticAssertExpr(__builtin_types_compatible_p(__typeof__(varname), typename), \
	 CppAsString(varname) " does not have type " CppAsString(typename)))
#else							/* !HAVE__BUILTIN_TYPES_COMPATIBLE_P */
#define AssertVariableIsOfType(varname, typename) \
	StaticAssertStmt(sizeof(varname) == sizeof(typename), \
	CppAsString(varname) " does not have type " CppAsString(typename))
#define AssertVariableIsOfTypeMacro(varname, typename) \
	(StaticAssertExpr(sizeof(varname) == sizeof(typename), \
	 CppAsString(varname) " does not have type " CppAsString(typename)))
#endif							/* HAVE__BUILTIN_TYPES_COMPATIBLE_P */



/*
 * The definitions for the request/response codes are kept in a separate file
 * for ease of use in third party programs.
 */
#include "protocol.h"

typedef struct
{
	struct sockaddr_storage addr;
	socklen_t	salen;
} SockAddr;

typedef struct
{
	int			family;
	SockAddr	addr;
} AddrInfo;

/* Configure the UNIX socket location for the well known port. */

#define UNIXSOCK_PATH(path, port, sockdir) \
	   (AssertMacro(sockdir), \
		AssertMacro(*(sockdir) != '\0'), \
		snprintf(path, sizeof(path), "%s/.s.PGSQL.%d", \
				 (sockdir), (port)))

/*
 * The maximum workable length of a socket path is what will fit into
 * struct sockaddr_un.  This is usually only 100 or so bytes :-(.
 *
 * For consistency, always pass a MAXPGPATH-sized buffer to UNIXSOCK_PATH(),
 * then complain if the resulting string is >= UNIXSOCK_PATH_BUFLEN bytes.
 * (Because the standard API for getaddrinfo doesn't allow it to complain in
 * a useful way when the socket pathname is too long, we have to test for
 * this explicitly, instead of just letting the subroutine return an error.)
 */
#define UNIXSOCK_PATH_BUFLEN sizeof(((struct sockaddr_un *) NULL)->sun_path)

/*
 * A host that looks either like an absolute path or starts with @ is
 * interpreted as a Unix-domain socket address.
 */
static inline bool
is_unixsock_path(const char *path)
{
	//return is_absolute_path(path) || path[0] == '@';
	return path;
}


#endif							/* PQCOMM_H */
