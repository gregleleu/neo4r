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
  pkg_load()
}

.onUnload <- function(libpath) {
  pkg_unload()
}

