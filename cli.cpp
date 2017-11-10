#include <cassandra.h>
#include <stdio.h>
#include <iostream>
#include "cmds.h"

using namespace std;

void loop(CassSession* session, const char* hosts);

int main(int argc, char* argv[]) {
	
    const char* hosts = "127.0.0.1";

    // use host provided on cli if given
    if (argc > 1) {
		hosts = argv[1];
	}
    
    // setup and connect to cluster
    CassFuture* connect_future = NULL;
    CassCluster* cluster = cass_cluster_new();
    CassSession* session = cass_session_new();

    // add contact points
    cass_cluster_set_contact_points(cluster, hosts);

    // provide the cluster object as configuration to connect the session
    connect_future = cass_session_connect(session, cluster);

    // connection success -> start cli loop
    if (cass_future_error_code(connect_future) == CASS_OK) loop(session, hosts);
    // connection fail -> handle error
    else cass_err(connect_future);

    cout << "\n Exiting Cassandra CLI...\n" << endl;

    // close connection, etc.
    cass_future_free(connect_future);
    cass_cluster_free(cluster);
    cass_session_free(session);

    return 0;
}

void loop(CassSession* session, const char* hosts) {
    
    cout << "\n Welcome to Cassandra CLI.\n" << endl;
    cout << "  Current host: " << hosts << endl;
    cout << "  Available commands:" << endl;
    cout << "   show, list, use, get, insert, quit\n" << endl;
    
    // store current keyspace and table to display in prompt
    string keyspace = "";
    string table = "";     
 
    for(;;) {
        
        // print prompt
        if (keyspace.length() < 1) cout << "cassandra> ";
        else cout << keyspace << ":" << table << "> ";

        string cmd;
        string param;
        string buffer;

        getline(cin, buffer);

        // split input
        cmd = buffer.substr(0, buffer.find(' '));
        param = buffer.substr(buffer.find(' ') + 1, buffer.find('\n'));
   
        // call matching handler function for command inputted from user
        if (cmd == "show") show(session); 
        else if (cmd == "list") list(session, keyspace);
        else if (cmd == "use") {
            string newkeyspace = param.substr(0, param.find('.'));
            string newtable = param.substr(param.find('.') + 1, param.find('\n'));
            use(session, &keyspace, &table, newkeyspace, newtable);
        }
        else if (cmd == "get") get(session, param, table);
        else if (cmd == "insert") {
            string key = param.substr(0, param.find(' '));
            string value = param.substr(param.find(' ') + 1, param.find('\n'));
            insert(session, keyspace, table, key, value);
        }
        else if (cmd == "quit" || cmd == "q") break;
        else cout << cmd << " is not a valid command." << endl;
    
    }

    
    return;
}
