include(ExternalProject)

set (LIBPAXOS_PREFIX "${CMAKE_BINARY_DIR}/libpaxos-prefix")
ExternalProject_Add(project_libpaxos
    SOURCE_DIR "${CMAKE_SOURCE_DIR}/deps/libpaxos"
    INSTALL_DIR "${LIBPAXOS_PREFIX}"
    CMAKE_ARGS "-DCMAKE_INSTALL_PREFIX:PATH=${LIBPAXOS_PREFIX}"
)

file(MAKE_DIRECTORY "${LIBPAXOS_PREFIX}/include")

add_library(evpaxos SHARED IMPORTED)

set_target_properties(evpaxos
    PROPERTIES
        IMPORTED_LOCATION "${LIBPAXOS_PREFIX}/lib/libevpaxos.so"
)

add_dependencies(evpaxos project_libpaxos)

target_include_directories(evpaxos
    INTERFACE
        "${LIBPAXOS_PREFIX}/include"
)
