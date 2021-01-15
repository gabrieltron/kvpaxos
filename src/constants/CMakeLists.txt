add_library(constants)

target_sources(
    constants
        PUBLIC
            constants.h
        PRIVATE
            constants.cpp
)
