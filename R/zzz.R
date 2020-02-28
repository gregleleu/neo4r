globalVariables(
  list(
    "name",
    "id",
    "label",
    "properties"
  )
)

## Init C++ neo4j client to use Bolt
.onLoad <- function(libname, pkgname) {
  res <- pkg_load()
  return(res)
}

.onUnload <- function(libpath) {
  res <- pkg_unload()
  return(res)
}

