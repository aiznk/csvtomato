#pragma once

#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>

#define DECL_ARRAY(NAME, NS, TYPE)\
	typedef struct {\
		size_t capa;\
		size_t len;\
		TYPE *array;\
	} NAME;\
	\
	NAME *\
	NS ## _new(void);\
	\
	void\
	NS ## _del(NAME *self);\
	\
	TYPE *\
	NS ## _esc_del(NAME *self);\
	\
	NAME *\
	NS ## _resize(NAME *self, size_t new_capa);\
	\
	NAME *\
	NS ## _push_back(NAME *self, TYPE ch);\
	\
	TYPE\
	NS ## _pop_back(NAME *self);\
	\
	void\
	NS ## _clear(NAME *self);\


#define DEF_ARRAY(NAME, NS, TYPE, NIL)\
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
		self->array = malloc(size);\
		if (!self->array) {\
			free(self);\
			return NULL;\
		}\
		self->array[0] = NIL;\
		self->capa = capa;\
		\
		return self;\
	}\
	\
	void\
	NS ## _del(NAME *self) {\
		if (self) {\
			free(self->array);\
			free(self);\
		}\
	}\
	\
	TYPE *\
	NS ## _esc_del(NAME *self) {\
		TYPE *str = self->array;\
		self->array = NULL;\
		NS ## _del(self);\
		return str;\
	}\
	\
	NAME *\
	NS ## _resize(NAME *self, size_t new_capa) {\
		size_t byte = sizeof(TYPE);\
		size_t size = byte * new_capa + byte;\
		TYPE *tmp = realloc(self->array, size);\
		if (!tmp) {\
			return NULL;\
		}\
		\
		self->array = tmp;\
		self->capa = new_capa;\
		\
		return self;\
	}\
	\
	NAME *\
	NS ## _push_back(NAME *self, TYPE elem) {\
		if (self->len >= self->capa) {\
			if (!NS ## _resize(self, self->capa*2)) {\
				return NULL;\
			}\
		}\
		\
		self->array[self->len++] = elem;\
		self->array[self->len] = NIL;\
		return self;\
	}\
	\
	TYPE\
	NS ## _pop_back(NAME *self) {\
		TYPE ret = NIL;\
		if (self->len) {\
			self->len--;\
			ret = self->array[self->len];\
			self->array[self->len] = NIL;\
		}\
		return ret;\
	}\
	\
	void\
	NS ## _clear(NAME *self) {\
		self->len = 0;\
	}\

