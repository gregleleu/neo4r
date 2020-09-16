#include <Rcpp.h>
#include "bolt/bolt.h"
using namespace Rcpp;


#define TRY(connection, code) {                                               \
int error_try = (code);                                                       \
if (error_try != BOLT_SUCCESS) {                                              \
  check_and_print_error(connection, BoltConnection_status(connection), NULL); \
  return error_try;                                                           \
}                                                                             \
}                                                              \

// https://github.com/neo4j-drivers/seabolt
// https://github.com/neo4j-drivers/seabolt/blob/8185d2798ac5944d807bd25fb76e65d6641eb128/docs/source/using.rst


// utils
int32_t check_and_print_error(BoltConnection* connection, BoltStatus* status, const char* error_text)
{
  int32_t error_code = BoltStatus_get_error(status);
  if (error_code==BOLT_SUCCESS) {
    return BOLT_SUCCESS;
  }

  if (error_code==BOLT_SERVER_FAILURE) {
    char string_buffer[4096];
    if (BoltValue_to_string(BoltConnection_failure(connection), string_buffer, 4096, connection)>4096) {
      string_buffer[4095] = 0;
    }
    REprintf("%s: %s", error_text==NULL ? "server failure" : error_text, string_buffer);
  }
  else {
    const char* error_ctx = BoltStatus_get_error_context(status);
    REprintf("%s (code: %04X, text: %s, context: %s)\n", error_text==NULL ? "Bolt failure" : error_text,
            error_code, BoltError_get_string(error_code), error_ctx);
  }
  return error_code;
}


// Load & Unload
// [[Rcpp::export]]
void pkg_load() {
  Bolt_startup();
  return;
}

// [[Rcpp::export]]
void pkg_unload() {
  Bolt_shutdown();
  return;
}



// Connector create
BoltConnector* create_connector()
{
  BoltAddress* address = BoltAddress_create("localhost", "7687");
  BoltValue* auth_token = BoltAuth_basic("neo4j", "coucou", NULL);
  BoltConfig* config = BoltConfig_create();
  BoltConfig_set_user_agent(config, "seabolt-cmake/1.7");

  BoltConnector* connector = BoltConnector_create(address, auth_token, config);

  BoltAddress_destroy(address);
  BoltValue_destroy(auth_token);
  BoltConfig_destroy(config);

  return connector;
}


// [[Rcpp::export]]
SEXP db_connect() {
  BoltConnector *connector = create_connector();

  // check if works
  BoltStatus* status = BoltStatus_create();
  check_and_print_error(NULL, status, "result");
  BoltStatus_destroy(status);

  return XPtr<BoltConnector>(connector);
}



// Disconnect
// [[Rcpp::export]]
void db_disconnect(SEXP r_conn) {
  Rcpp::XPtr<BoltConnector>connector(r_conn);
  BoltConnector_destroy(connector);
  return;
}


// Get cypher

int32_t run_cypher(BoltConnection* connection, const char* cypher, const char* p_name, int64_t p_value)
{
  TRY(connection, BoltConnection_set_run_cypher(connection, cypher, strlen(cypher), 1));
  BoltValue_format_as_Integer(BoltConnection_set_run_cypher_parameter(connection, 0, p_name, strlen(p_name)), p_value);
  TRY(connection, BoltConnection_load_run_request(connection));
  BoltRequest run = BoltConnection_last_request(connection);

  TRY(connection, BoltConnection_load_pull_request(connection, -1));
  BoltRequest pull_all = BoltConnection_last_request(connection);

  TRY(connection, BoltConnection_send(connection));

  char string_buffer[4096];
  if (BoltConnection_fetch_summary(connection, run)<0 || !BoltConnection_summary_success(connection)) {
    return check_and_print_error(connection, BoltConnection_status(connection), "cypher execution failed");
  }

  const BoltValue* field_names = BoltConnection_field_names(connection);
  for (int i = 0; i<BoltValue_size(field_names); i++) {
    BoltValue* field_name = BoltList_value(field_names, i);
    if (i>0) {
      printf("\t");
    }
    if (BoltValue_to_string(field_name, string_buffer, 4096, connection)>4096) {
      string_buffer[4095] = 0;
    }
    printf("%-12s", string_buffer);
  }
  printf("\n");

  while (BoltConnection_fetch(connection, pull_all)>0) {
    const BoltValue* field_values = BoltConnection_field_values(connection);
    for (int i = 0; i<BoltValue_size(field_values); i++) {
      BoltValue* field_value = BoltList_value(field_values, i);
      if (i>0) {
        printf("\t");
      }
      if (BoltValue_to_string(field_value, string_buffer, 4096, connection)>4096) {
        string_buffer[4095] = 0;
      }
      printf("%s", string_buffer);
    }
    printf("\n");
  }

  return check_and_print_error(connection, BoltConnection_status(connection), "cypher execution failed");
}


// [[Rcpp::export]]
void test_query(SEXP r_conn) {
  // convert connector
  Rcpp::XPtr<BoltConnector>connector(r_conn);

  // get connection
  BoltStatus* status = BoltStatus_create();
  BoltConnection* connection = BoltConnector_acquire(connector, BOLT_ACCESS_MODE_READ, status);
  if (connection!=NULL) {
    run_cypher(connection, "UNWIND RANGE(1, $x) AS N RETURN N", "x", 5);
  }
  else {
    check_and_print_error(NULL, status, "unable to acquire connection");
  }
  BoltConnector_release(connector, connection);
  BoltStatus_destroy(status);

  return;
}











