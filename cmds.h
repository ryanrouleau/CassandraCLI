#ifndef CMDS_H
#define CMDS_H

#include <cassandra.h>
#include <stdio.h>
#include <iostream>

using namespace std;

CassFuture* exec_query(CassSession* session, string query);
void cass_err(CassFuture* session);
void show(CassSession* session);
void list(CassSession* session, string keyspace);
void use(CassSession* session, string* keyspace, string* table, string newkeyspace, string newtable);
void get(CassSession* session, string key, string table);
void insert(CassSession* session, string keyspace, string table, string key, string value);

#endif
