cmake_minimum_required(VERSION 3.15)

project(bullet_trade_huaxin_bridge VERSION 0.1.0 LANGUAGES CXX)

set(BT_HUAXIN_MODE "offline_fake" CACHE STRING "offline_fake or trader")
set_property(CACHE BT_HUAXIN_MODE PROPERTY STRINGS offline_fake trader)
set(BT_HUAXIN_SDK_INCLUDE_DIR "" CACHE PATH "TORA Trader SDK include directory")
set(BT_HUAXIN_SDK_LIBRARY_DIR "" CACHE PATH "TORA Trader SDK library directory")
set(BT_HUAXIN_TRADER_LIBRARY "libtraderapi.so" CACHE STRING "TORA Trader runtime library filename")

if(BT_HUAXIN_MODE STREQUAL "offline_fake")
    set(BT_HUAXIN_BRIDGE_SOURCE src/bt_huaxin_bridge.cpp)
elseif(BT_HUAXIN_MODE STREQUAL "trader")
    if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
        message(FATAL_ERROR "Trader bridge currently supports Linux only")
    endif()
    if(NOT IS_DIRECTORY "${BT_HUAXIN_SDK_INCLUDE_DIR}")
        message(FATAL_ERROR "BT_HUAXIN_SDK_INCLUDE_DIR must be an existing directory")
    endif()
    if(NOT IS_DIRECTORY "${BT_HUAXIN_SDK_LIBRARY_DIR}")
        message(FATAL_ERROR "BT_HUAXIN_SDK_LIBRARY_DIR must be an existing directory")
    endif()
    set(BT_HUAXIN_VENDOR_LIBRARY "${BT_HUAXIN_SDK_LIBRARY_DIR}/${BT_HUAXIN_TRADER_LIBRARY}")
    if(NOT EXISTS "${BT_HUAXIN_VENDOR_LIBRARY}")
        message(FATAL_ERROR "TORA Trader runtime library was not found")
    endif()
    foreach(BT_HUAXIN_REQUIRED_HEADER
        TORATstpTraderApi.h
        TORATstpUserApiDataType.h
        TORATstpUserApiStruct.h
    )
        if(NOT EXISTS "${BT_HUAXIN_SDK_INCLUDE_DIR}/${BT_HUAXIN_REQUIRED_HEADER}")
            message(FATAL_ERROR "Required TORA Trader header was not found: ${BT_HUAXIN_REQUIRED_HEADER}")
        endif()
    endforeach()
    set(BT_HUAXIN_BRIDGE_SOURCE src/bt_huaxin_trader_bridge.cpp)
else()
    message(FATAL_ERROR "BT_HUAXIN_MODE must be offline_fake or trader")
endif()

add_library(
    bullet_trade_huaxin
    SHARED
    ${BT_HUAXIN_BRIDGE_SOURCE}
)

target_include_directories(
    bullet_trade_huaxin
    PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_compile_features(bullet_trade_huaxin PRIVATE cxx_std_17)
target_compile_definitions(bullet_trade_huaxin PRIVATE BT_HUAXIN_BUILDING_BRIDGE=1)

if(BT_HUAXIN_MODE STREQUAL "trader")
    target_include_directories(
        bullet_trade_huaxin
        SYSTEM PRIVATE
        ${BT_HUAXIN_SDK_INCLUDE_DIR}
    )
    # 官方库当前没有 SONAME；按文件名链接可避免把构建机 SDK 绝对路径写入 DT_NEEDED。
    target_link_directories(bullet_trade_huaxin PRIVATE ${BT_HUAXIN_SDK_LIBRARY_DIR})
    target_link_libraries(bullet_trade_huaxin PRIVATE "-l:${BT_HUAXIN_TRADER_LIBRARY}")
endif()

set_target_properties(
    bullet_trade_huaxin
    PROPERTIES
    CXX_EXTENSIONS OFF
    CXX_VISIBILITY_PRESET hidden
    VISIBILITY_INLINES_HIDDEN YES
    POSITION_INDEPENDENT_CODE ON
    OUTPUT_NAME bullet_trade_huaxin
)

if(BT_HUAXIN_MODE STREQUAL "trader")
    set_target_properties(
        bullet_trade_huaxin
        PROPERTIES
        BUILD_RPATH "${BT_HUAXIN_SDK_LIBRARY_DIR}"
        INSTALL_RPATH "\$ORIGIN/vendor"
        INSTALL_RPATH_USE_LINK_PATH FALSE
    )
endif()

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
    if(BT_HUAXIN_MODE STREQUAL "trader")
        target_link_options(bullet_trade_huaxin PRIVATE -Wl,--no-undefined)
    endif()
endif()

install(
    TARGETS bullet_trade_huaxin
    LIBRARY DESTINATION lib
    RUNTIME DESTINATION lib
    ARCHIVE DESTINATION lib
)
