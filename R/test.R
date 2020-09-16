#' Test connection
#'
#' @param url A NEO4JAPI connection object
#'
#' @return the result from the Neo4J Call
#' @export

test_con <- function() {

  out <- db_connect()

  return(out)
}

tmp <- db_connect()

test_query(tmp)

db_disconnect(tmp)
