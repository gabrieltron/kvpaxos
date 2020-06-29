include(ExternalProject)

set (LIBMETIS_PREFIX "${CMAKE_BINARY_DIR}/libmetis-prefix")

ExternalProject_Add(project_libmetis
    SOURCE_DIR
        "${CMAKE_SOURCE_DIR}/deps/METIS"
    CONFIGURE_COMMAND
        cd ${CMAKE_SOURCE_DIR}/deps/METIS && make config prefix=${LIBMETIS_PREFIX}
    BUILD_COMMAND
        ""
    INSTALL_COMMAND
        cd ${CMAKE_SOURCE_DIR}/deps/METIS && make -C build install
)

file(MAKE_DIRECTORY "${LIBMETIS_PREFIX}/include")
add_library(metis SHARED IMPORTED)

set_target_properties(metis
    PROPERTIES
        IMPORTED_LOCATION "${LIBMETIS_PREFIX}/lib/libmetis.a"
)

add_dependencies(metis project_libmetis)

target_include_directories(metis
    INTERFACE
        "${LIBMETIS_PREFIX}/include"
)
