#include <Rcpp.h>
#include <errno.h>
#include <neo4j-client.h>
using namespace Rcpp;


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





