add_subdirectory(request)
add_subdirectory(evclient)
add_subdirectory(toml11)
add_subdirectory(libpaxos)

add_executable(main)

target_sources(
    main
        PRIVATE
            main.cpp
)

target_link_libraries(
    main
        PRIVATE
            toml11
            request
            evclient
            libpaxos
)
