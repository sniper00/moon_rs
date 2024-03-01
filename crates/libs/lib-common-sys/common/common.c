#define PLATFORM_UNKNOWN 0
#define PLATFORM_WINDOWS 1
#define PLATFORM_LINUX 2
#define PLATFORM_MAC 3

#define TARGET_PLATFORM PLATFORM_UNKNOWN

// mac
#if defined(__APPLE__) && (defined(__GNUC__) || defined(__xlC__) || defined(__xlc__))
#undef TARGET_PLATFORM
#define TARGET_PLATFORM PLATFORM_MAC
#endif

// win32
#if !defined(SAG_COM) && (defined(WIN32) || defined(_WIN32) || defined(__WIN32__) || defined(__NT__))
#if defined(WINCE) || defined(_WIN32_WCE)
// win ce
#else
#undef TARGET_PLATFORM
#define TARGET_PLATFORM PLATFORM_WINDOWS
#endif
#endif

#if !defined(SAG_COM) && (defined(WIN64) || defined(_WIN64) || defined(__WIN64__))
#undef TARGET_PLATFORM
#define TARGET_PLATFORM PLATFORM_WINDOWS
#endif

// linux
#if defined(__linux__) || defined(__linux)
#undef TARGET_PLATFORM
#define TARGET_PLATFORM PLATFORM_LINUX
#endif

//////////////////////////////////////////////////////////////////////////
// post configure
//////////////////////////////////////////////////////////////////////////

// check user set platform
#if !TARGET_PLATFORM
#error "Cannot recognize the target platform; are you targeting an unsupported platform?"
#endif

#if TARGET_PLATFORM == PLATFORM_WINDOWS
#include <WinSock2.h>
#include <process.h> //  _get_pid support
#include <stdio.h>
#include <stdarg.h>
#include <signal.h>
#define strnicmp _strnicmp

inline int vsnprintf_(char *buffer,
                      size_t count,
                      const char *format,
                      va_list argptr)
{
    return vsnprintf_s(buffer, count, _TRUNCATE, format, argptr);
}

#ifdef _WIN64
typedef __int64 ssize_t;
#else
typedef _W64 int ssize_t;
#endif
#else
#include <sys/syscall.h>
#include <unistd.h>
#define vsnprintf_ vsnprintf
#endif
