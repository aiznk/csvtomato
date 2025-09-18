#pragma once

#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>

#define DECL_STRING(NAME, NS, TYPE)\
	typedef struct {\
		size_t capa;\
		size_t len;\
		TYPE *str;\
	} NAME;\
	\
	NAME *\
	NS ## _new(void);\
	\
	void\
	NS ## _del(NAME *self);\
	\
	NAME *\
	NS ## _resize(NAME *self, size_t newcapa);\
	\
	NAME *\
	NS ## _push_back(NAME *self, TYPE ch);\
	\
	TYPE\
	NS ## _pop_back(NAME *self);\
	\
	void\
	NS ## _clear(NAME *self);\
	\
	NAME *\
	NS ## _append(NAME *self, const TYPE *str);\

#define DEF_STRING(NAME, NS, TYPE, NIL)\
	NAME *\
	NS ## _new(void) {\
		NAME *self = calloc(1, sizeof(*self));\
		if (!self) {\
			return NULL;\
		}\
		\
		size_t byte = sizeof(TYPE);\
		size_t capa = 4;\
		size_t size = byte * capa + byte;\
		self->str = malloc(size);\
		if (!self->str) {\
			free(self);\
			return NULL;\
		}\
		self->str[0] = NIL;\
		self->capa = capa;\
		\
		return self;\
	}\
	\
	void\
	NS ## _del(NAME *self) {\
		if (self) {\
			free(self->str);\
			free(self);\
		}\
	}\
	\
	NAME *\
	NS ## _resize(NAME *self, size_t newcapa) {\
		size_t byte = sizeof(TYPE);\
		size_t size = byte * newcapa + byte;\
		TYPE *tmp = realloc(self->str, size);\
		if (!tmp) {\
			return NULL;\
		}\
		\
		self->str = tmp;\
		self->capa = newcapa;\
		\
		return self;\
	}\
	\
	NAME *\
	NS ## _push_back(NAME *self, TYPE ch) {\
		if (self->len >= self->capa) {\
			if (!NS ## _resize(self, self->capa*2)) {\
				return NULL;\
			}\
		}\
		\
		self->str[self->len++] = ch;\
		self->str[self->len] = NIL;\
		return self;\
	}\
	\
	TYPE\
	NS ## _pop_back(NAME *self) {\
		TYPE ret = NIL;\
		if (self->len) {\
			self->len--;\
			ret = self->str[self->len];\
			self->str[self->len] = NIL;\
		}\
		return ret;\
	}\
	\
	void\
	NS ## _clear(NAME *self) {\
		self->len = 0;\
	}\
	\
	NAME *\
	NS ## _append(NAME *self, const TYPE *str) {\
		for (const TYPE *p = str; *p; p++) {\
			if (!NS ## _push_back(self, *p)) {\
				return NULL;\
			}\
		}\
		return self;\
	}\

