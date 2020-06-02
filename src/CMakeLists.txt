add_subdirectory(request)
add_subdirectory(evclient)

add_executable(main)

target_sources(
    main
        PRIVATE
            main.cpp
)

target_link_libraries(
    main
        PRIVATE
            CONAN_PKG::toml11
            request
            evclient
            evpaxos
)
