add_library(compresser)

target_sources(
    compresser
        PUBLIC
            compresser.h
        PRIVATE
            compresser.cpp
)

target_include_directories(
    compresser
        PUBLIC
            "${CMAKE_SOURCE_DIR}/src"
)

target_link_libraries(
    compresser
        PUBLIC
            CONAN_PKG::zlib
            constants
)
