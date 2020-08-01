add_library(compresser)

target_sources(
    compresser
        PUBLIC
            compresser.h
        PRIVATE
            compresser.cpp
)

target_link_libraries(
    compresser
        PUBLIC
            CONAN_PKG::zlib
)
