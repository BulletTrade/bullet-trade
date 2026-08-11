cmake_minimum_required(VERSION 3.15)

project(bullet_trade_huaxin_bridge VERSION 0.1.0 LANGUAGES CXX)

add_library(
    bullet_trade_huaxin
    SHARED
    src/bt_huaxin_bridge.cpp
)

target_include_directories(
    bullet_trade_huaxin
    PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_compile_features(bullet_trade_huaxin PRIVATE cxx_std_17)
target_compile_definitions(bullet_trade_huaxin PRIVATE BT_HUAXIN_BUILDING_BRIDGE=1)

set_target_properties(
    bullet_trade_huaxin
    PROPERTIES
    CXX_EXTENSIONS OFF
    CXX_VISIBILITY_PRESET hidden
    VISIBILITY_INLINES_HIDDEN YES
    POSITION_INDEPENDENT_CODE ON
    OUTPUT_NAME bullet_trade_huaxin
)

if(MSVC)
    target_compile_options(bullet_trade_huaxin PRIVATE /W4 /EHsc)
else()
    target_compile_options(
        bullet_trade_huaxin
        PRIVATE
        -Wall
        -Wextra
        -Wpedantic
        -Wconversion
        -Wsign-conversion
    )
endif()

install(
    TARGETS bullet_trade_huaxin
    LIBRARY DESTINATION lib
    RUNTIME DESTINATION lib
    ARCHIVE DESTINATION lib
)
