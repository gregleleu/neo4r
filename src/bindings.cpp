#include <Rcpp.h>
#include <bolt/bolt.h>
using namespace Rcpp;


#define TRY(connection, code) {                                               \
int error_try = (code);                                                       \
if (error_try != BOLT_SUCCESS) {                                              \
  check_and_print_error(connection, BoltConnection_status(connection), NULL); \
  return error_try;                                                           \
}                                                                             \
}

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
    fprintf(stderr, "%s: %s", error_text==NULL ? "server failure" : error_text, string_buffer);
  }
  else {
    const char* error_ctx = BoltStatus_get_error_context(status);
    fprintf(stderr, "%s (code: %04X, text: %s, context: %s)\n", error_text==NULL ? "Bolt failure" : error_text,
            error_code, BoltError_get_string(error_code), error_ctx);
  }
  return error_code;
}

BoltConnector* create_connector()
{
  BoltAddress* address = BoltAddress_create("localhost", "7687");
  BoltValue* auth_token = BoltAuth_basic("neo4j", "password", NULL);
  BoltConfig* config = BoltConfig_create();
  BoltConfig_set_user_agent(config, "seabolt-cmake/1.7");

  BoltConnector* connector = BoltConnector_create(address, auth_token, config);

  BoltAddress_destroy(address);
  BoltValue_destroy(auth_token);
  BoltConfig_destroy(config);

  return connector;
}

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

int main(int argc, char** argv) {
  Bolt_startup();

  BoltConnector *connector = create_connector();

  BoltStatus* status = BoltStatus_create();

  BoltConnection* connection = BoltConnector_acquire(connector, BOLT_ACCESS_MODE_READ, status);
  if (connection!=NULL) {
    run_cypher(connection, "UNWIND RANGE(1, $x) AS N RETURN N", "x", 5);
  }
  else {
    check_and_print_error(NULL, status, "unable to acquire connection");
  }

  BoltStatus_destroy(status);

  BoltConnector_release(connector, connection);

  BoltConnector_destroy(connector);

  Bolt_shutdown();
}










// Utils
void neo4j_perror_to_r(int errnum, const char *message)
{
  char buf[1024];
  REprintf("%s%s%s\n", (message != NULL)? message : "",
           (message != NULL)? ": " : "",
           neo4j_strerror(errnum, buf, sizeof(buf)));
}

// Load & Unload
// [[Rcpp::export]]
IntegerVector pkg_load() {
  int res = neo4j_client_init();
  return Rcpp::wrap(res);
}

IntegerVector pkg_unload() {
  int res = neo4j_client_cleanup();
  return Rcpp::wrap(res);
}

// Connect
// [[Rcpp::export]]
/* use NEO4J_INSECURE when connecting to disable TLS */
SEXP db_connect(SEXP rurl) {
  const char * url = Rcpp::as<const char *>( rurl );

  neo4j_connection_t *connection = neo4j_connect(url, NULL, NEO4J_INSECURE);
  if (connection == NULL)
  {
    //Rcout << "\nError connecting";
    neo4j_perror_to_r(errno, "Error connecting");
    return R_NilValue;
  }


  return XPtr<neo4j_connection_t>(connection);
}



// Disconnect
// [[Rcpp::export]]
/* use NEO4J_INSECURE when connecting to disable TLS */
IntegerVector db_disconnect(SEXP conn) {
  Rcpp::XPtr<neo4j_connection_t>w(conn) ;
  int res = neo4j_close(w);
  return Rcpp::wrap(res);
}





