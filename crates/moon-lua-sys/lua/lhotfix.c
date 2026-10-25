/* Native hotfix helpers, included by onelua.c after the Lua core. */
#define LUA_LIB

#include "lprefix.h"

#include <lua.h>
#include <lauxlib.h>

#include <lapi.h>
#include <lstate.h>
#include <lobject.h>
#include <lfunc.h>
#include <lgc.h>
#include <lopcodes.h>
#include <lopnames.h>

/* A path is a sequence of little-endian uint32 child indices. The owning
 * closure keeps the complete prototype tree alive; every step is checked. */
static Proto *
selectproto(lua_State *L) {
	Proto *p = ((const LClosure *)lua_topointer(L, 1))->p;
	if (lua_type(L, 2) == LUA_TSTRING && !lua_isnumber(L, 2)) {
		size_t length, offset;
		const unsigned char *path = (const unsigned char *)lua_tolstring(L, 2, &length);
		luaL_argcheck(L, length % 4 == 0, 2, "invalid prototype path");
		for (offset = 0; offset < length; offset += 4) {
			l_uint32 n = (l_uint32)path[offset] | ((l_uint32)path[offset + 1] << 8)
				| ((l_uint32)path[offset + 2] << 16) | ((l_uint32)path[offset + 3] << 24);
			luaL_argcheck(L, n > 0 && n <= (l_uint32)p->sizep, 2, "prototype path out of range");
			p = p->p[n - 1];
		}
	} else {
		lua_Integer n = luaL_optinteger(L, 2, 0);
		if (n < 0 || n > p->sizep) return NULL;
		if (n) p = p->p[n - 1];
	}
	return p;
}

static int
lclone(lua_State *L) {
	if (!lua_isfunction(L, 1) || lua_iscfunction(L,1))
		return luaL_error(L, "Need lua function");
	Proto *p = selectproto(L);
	if (!p) return 0;
	luaL_checkstack(L, 1, NULL);

	lua_lock(L);
	LClosure *cl = luaF_newLclosure(L, p->sizeupvalues);
	cl->p = p;
	setclLvalue2s(L, L->top.p++, cl);
	luaF_initupvals(L, cl);
	lua_unlock(L);

	return 1;
}

static int
lproto(lua_State *L) {
	if (!lua_isfunction(L, 1) || lua_iscfunction(L,1))
		return 0;
	Proto *p = selectproto(L);
	if (!p) return 0;
	lua_pushlightuserdata(L, p);
	lua_pushinteger(L, p->sizep);
	return 2;
}

/* Resolve for inspection/state binding; debug APIs retain their usual meaning
 * for an old closure or an already-running frame. */
static int
lresolve(lua_State *L) {
	luaL_checktype(L, 1, LUA_TFUNCTION);
	lua_pushvalue(L, 1);
	if (!lua_iscfunction(L, 1)) {
		LClosure *cl = (LClosure *)lua_topointer(L, 1);
		setclLvalue2s(L, L->top.p - 1, currentclosure(cl));
	}
	return 1;
}

/* Called only after checking that 'to' is unbound or belongs to the same cell.
** Allocate before changing any existing reference, so failure is reversible. */
static void redirectclosure (lua_State *L, LClosure *from, LClosure *to) {
  UpVal *slot = from->redirect;
  if (slot == NULL) {
    GCObject *o = luaC_newobj(L, LUA_VUPVAL, sizeof(UpVal));
    slot = gco2upv(o);
    slot->v.p = &slot->u.value;
    setclLvalue(L, slot->v.p, from);
    from->redirect = slot;
    luaC_objbarrier(L, from, slot);
  }
  to->redirect = slot;
  luaC_objbarrier(L, to, slot);
  setclLvalue(L, slot->v.p, to);
  luaC_objbarrier(L, slot, to);
}


static int
lredirect(lua_State *L) {
	if (!lua_isfunction(L, 1) || lua_iscfunction(L, 1) ||
		!lua_isfunction(L, 2) || lua_iscfunction(L, 2))
		return luaL_error(L, "redirect requires two Lua functions");
	LClosure *from = (LClosure *)lua_topointer(L, 1);
	LClosure *to = (LClosure *)lua_topointer(L, 2);
	if (to->redirect && to->redirect != from->redirect)
		return luaL_error(L, "cannot merge redirected closure instances");
	lua_lock(L);
	redirectclosure(L, from, to);
	lua_unlock(L);
	return 0;
}

/* Standard prototype/debug information; bindings are analyzed in hotfix.lua. */
static int
linfo(lua_State *L) {
	const Proto *p;
	int i;
	if (!lua_isfunction(L, 1) || lua_iscfunction(L, 1))
		return luaL_error(L, "Need lua function");
	p = selectproto(L);
	if (!p) return 0;
	lua_createtable(L, 0, 5);
	lua_pushlightuserdata(L, (void *)p);
	lua_setfield(L, -2, "proto");
	lua_pushinteger(L, p->sizep);
	lua_setfield(L, -2, "children");
	lua_createtable(L, p->sizelocvars, 0);
	for (i = 0; i < p->sizelocvars; i++) {
		lua_pushstring(L, p->locvars[i].varname ? getstr(p->locvars[i].varname) : "");
		lua_rawseti(L, -2, i + 1);
	}
	lua_setfield(L, -2, "locals");
	lua_createtable(L, p->sizelocvars, 0);
	for (i = 0; i < p->sizelocvars; i++) {
		lua_pushboolean(L, p->locvars[i].endpc == p->sizecode);
		lua_rawseti(L, -2, i + 1);
	}
	lua_setfield(L, -2, "localscope");
	lua_createtable(L, p->sizeupvalues, 0);
	for (i = 0; i < p->sizeupvalues; i++) {
		const Upvaldesc *uv = &p->upvalues[i];
		lua_createtable(L, 0, 3);
		lua_pushstring(L, uv->name ? getstr(uv->name) : NULL);
		lua_setfield(L, -2, "name");
		lua_pushboolean(L, uv->instack);
		lua_setfield(L, -2, "instack");
		lua_pushinteger(L, uv->idx + 1);
		lua_setfield(L, -2, "index");
		lua_rawseti(L, -2, i + 1);
	}
	lua_setfield(L, -2, "upvalues");
	return 1;
}

/* Read-only compiler output for the project's binding analyzer. Nothing here
 * evaluates a register, a constant table, or any module initialization. */
static int
lcode(lua_State *L) {
	if (!lua_isfunction(L, 1) || lua_iscfunction(L, 1))
		return luaL_error(L, "Need lua function");
	const Proto *p = selectproto(L);
	if (!p) return 0;
	lua_pushlstring(L, (const char *)p->code, p->sizecode * sizeof(Instruction));
	lua_createtable(L, p->sizek, 0);
	for (int i = 0; i < p->sizek; i++) {
		setobj2s(L, L->top.p, &p->k[i]);
		L->top.p++;
		lua_rawseti(L, -2, i + 1);
	}
	lua_createtable(L, p->sizelocvars * 2, 0);
	for (int i = 0; i < p->sizelocvars; i++) {
		lua_pushinteger(L, p->locvars[i].startpc);
		lua_rawseti(L, -2, i * 2 + 1);
		lua_pushinteger(L, p->locvars[i].endpc);
		lua_rawseti(L, -2, i * 2 + 2);
	}
	lua_pushinteger(L, p->maxstacksize);
	return 4;
}

LUAMOD_API int
luaopen_hotfix_core(lua_State *L) {
	luaL_checkversion(L);
	luaL_Reg l[] = {
		{ "clone", lclone },
		{ "proto", lproto },
		{ "info", linfo },
		{ "resolve", lresolve },
		{ "redirect", lredirect },
		{ "code", lcode },
		{ NULL, NULL },
	};
	luaL_newlib(L, l);
	lua_newtable(L);
	lua_newtable(L);
	for (int i = 0; i < NUM_OPCODES; i++) {
		lua_pushinteger(L, i); lua_setfield(L, -3, opnames[i]);
		lua_pushinteger(L, luaP_opmodes[i]); lua_rawseti(L, -2, i);
	}
	lua_setfield(L, -3, "opmodes");
	lua_setfield(L, -2, "opcodes");
	return 1;
}
