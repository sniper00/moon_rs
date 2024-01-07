#ifndef _ATOMIC_H___
#define _ATOMIC_H___

#if defined (__cplusplus)
#include <atomic>
#define STD_ std::
#define atomic_value_type_(p, v) decltype((p)->load())(v) 
#else
#include <stdatomic.h>
#define STD_
#define atomic_value_type_(p, v) v
#endif

#define ATOM_INT  STD_ atomic_int
#define ATOM_POINTER STD_ atomic_uintptr_t
#define ATOM_SIZET STD_ atomic_size_t
#define ATOM_ULONG STD_ atomic_ulong
#define ATOM_INIT(ref, v) STD_ atomic_init(ref, v)
#define ATOM_LOAD(ptr) STD_ atomic_load(ptr)
#define ATOM_STORE(ptr, v) STD_ atomic_store(ptr, v)

static inline int
ATOM_CAS(STD_ atomic_int *ptr, int oval, int nval) {
	return STD_ atomic_compare_exchange_weak(ptr, &(oval), nval);
}

static inline int
ATOM_CAS_SIZET(STD_ atomic_size_t *ptr, size_t oval, size_t nval) {
	return STD_ atomic_compare_exchange_weak(ptr, &(oval), nval);
}

static inline int
ATOM_CAS_ULONG(STD_ atomic_ulong *ptr, unsigned long oval, unsigned long nval) {
	return STD_ atomic_compare_exchange_weak(ptr, &(oval), nval);
}

static inline int
ATOM_CAS_POINTER(STD_ atomic_uintptr_t *ptr, uintptr_t oval, uintptr_t nval) {
	return STD_ atomic_compare_exchange_weak(ptr, &(oval), nval);
}

#define ATOM_FINC(ptr) STD_ atomic_fetch_add(ptr, atomic_value_type_(ptr,1))
#define ATOM_FDEC(ptr) STD_ atomic_fetch_sub(ptr, atomic_value_type_(ptr, 1))
#define ATOM_FADD(ptr,n) STD_ atomic_fetch_add(ptr, atomic_value_type_(ptr, n))
#define ATOM_FSUB(ptr,n) STD_ atomic_fetch_sub(ptr, atomic_value_type_(ptr, n))
#define ATOM_FAND(ptr,n) STD_ atomic_fetch_and(ptr, atomic_value_type_(ptr, n))
#endif