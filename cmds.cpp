#include "cmds.h"
#include <algorithm>
#include <vector>
// key is a column
// create table, create column, place value in column
void show(CassSession* session) {
    string query = "SELECT keyspace_name FROM system_schema.keyspaces";

    CassFuture* future_result = exec_query(session, query);
    
    if (cass_future_error_code(future_result) == CASS_OK) {
 
        // create iterator from result 
        const CassResult* result = cass_future_get_result(future_result);
        CassIterator* iter = cass_iterator_from_result(result);
       
        // loop through each row
        while (cass_iterator_next(iter)) {
            
            // get row
            const CassRow* row = cass_iterator_get_row(iter);
            
            // get column value
            const CassValue* val = cass_row_get_column_by_name(row, "keyspace_name");
            
            // create string from value 
            const char* result_str;
            size_t result_str_len;
            cass_value_get_string(val, &result_str, &result_str_len);

            // print to shell
            cout << result_str << endl;
        
        }

        // free memory
        cass_result_free(result);
        cass_iterator_free(iter);
    }
    else cass_err(future_result);

    cass_future_free(future_result);

    return;
}

void use(CassSession* session, string* keyspace, string* table, string newkeyspace, string newtable) {
    string query = "USE " + newkeyspace;

    CassFuture* future_result = exec_query(session, query);

    if (cass_future_error_code(future_result) == CASS_OK) {
        // if good set passed in keyspace and table as current
        // hope to good table is an actual table like wtf
        *keyspace = newkeyspace;
        *table = newtable;
    }
    else cass_err(future_result);

    cass_future_free(future_result);
    
    return;
}

// update to list table columns as well ! 
void list(CassSession* session, string keyspace) {
    string query = "SELECT table_name FROM system_schema.columns WHERE keyspace_name = '" + keyspace + "'";

    CassFuture* future_result = exec_query(session, query);

    if (cass_future_error_code(future_result) == CASS_OK) {
        const CassResult* result = cass_future_get_result(future_result);
        CassIterator* iter = cass_iterator_from_result(result);

        vector<string> tables;

        while(cass_iterator_next(iter)) {
            const CassRow* row = cass_iterator_get_row(iter);
            const CassValue* val = cass_row_get_column_by_name(row, "table_name");

            const char* result_str;
            size_t result_str_len;
            cass_value_get_string(val, &result_str, &result_str_len);
            
            if (find(tables.begin(), tables.end(), result_str) == tables.end()) {
                tables.push_back(result_str);
            }

        }

        cout << "Tables in " << keyspace << ":\n" << endl;
        for (int i = 0; i < tables.size(); i++) {
            cout << " " << tables[i] << endl;
        }
        cout << endl;
        cass_result_free(result);
        cass_iterator_free(iter);
    }
    else cass_err(future_result);

    cass_future_free(future_result);
    return;
}

void get(CassSession* session, string key, string table) {
    string query = "SELECT " + key + " FROM " + table;

    CassFuture* future_result = exec_query(session, query);

    if (cass_future_error_code(future_result) == CASS_OK) {
        const CassResult* result = cass_future_get_result(future_result);
        const CassRow* row = cass_result_first_row(result);
        const CassValue* val = cass_row_get_column_by_name(row, key.c_str());

        cass_int32_t int_value;
        cass_value_get_int32(val, &int_value);

        cout << int_value << endl;
        
        cass_result_free(result);
    }
    else cass_err(future_result);

    cass_future_free(future_result);
    return;
}

void insert(CassSession* session, string keyspace, string table, string key, string value) {
    string query = "INSERT INTO " + keyspace + "." + table + " (" + key + ") " + "VALUES(" + value + ")";

    CassFuture* future_result = exec_query(session, query);

    if (cass_future_error_code(future_result) != CASS_OK) cass_err(future_result);

    cass_future_free(future_result);
    
    return;
}

CassFuture* exec_query(CassSession* session, string queryString) {
    
    // construct cassandra query
    CassStatement* query = cass_statement_new(queryString.c_str(), 0);

    // execute
    CassFuture* future = cass_session_execute(session, query);
    
    // block until complete
    cass_future_wait(future);

    // free memory
    cass_statement_free(query);

    return future;
}


void cass_err(CassFuture* future) {
    const char* msg;
    size_t msg_length;
    cass_future_error_message(future, &msg, &msg_length);
    fprintf(stderr, "Unable to connect: '%.*s'\n", (int)msg_length, msg);
}
