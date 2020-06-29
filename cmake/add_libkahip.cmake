include(ExternalProject)

set (LIBKAHIP_PREFIX "${CMAKE_BINARY_DIR}/libkahip-prefix")
ExternalProject_Add(project_libkahip
    SOURCE_DIR
        "${CMAKE_SOURCE_DIR}/deps/KaHIP"
    INSTALL_DIR
        "${LIBKAHIP_PREFIX}"
    CMAKE_ARGS
        "-DCMAKE_INSTALL_PREFIX:PATH=${LIBKAHIP_PREFIX}"
        "-DCMAKE_PROJECT_KaHIP_INCLUDE=${CMAKE_SOURCE_DIR}/cmake/libkahip_install_header.cmake"
)

file(MAKE_DIRECTORY "${LIBKAHIP_PREFIX}/include")

add_library(kahip SHARED IMPORTED)

set_target_properties(kahip
    PROPERTIES
        IMPORTED_LOCATION "${LIBKAHIP_PREFIX}/lib/libinterface.so"
)

add_dependencies(kahip project_libkahip)

target_include_directories(kahip
    INTERFACE
        "${LIBKAHIP_PREFIX}/include"
)
