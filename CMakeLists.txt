cmake_minimum_required(VERSION 3.20)
project(trading_simulator LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(TS_BUILD_TESTS "Build unit tests" ON)
option(TS_BUILD_ADAPTERS "Build Kafka/Redis adapters (needs vcpkg dependencies)" ON)

function(ts_warnings target)
  if(MSVC)
    target_compile_options(${target} PRIVATE /W4 /permissive-)
  else()
    target_compile_options(${target} PRIVATE -Wall -Wextra -Wpedantic)
  endif()
endfunction()

add_library(rfq_core src/engine/rfq_engine.cpp)
target_include_directories(rfq_core PUBLIC src)
target_compile_features(rfq_core PUBLIC cxx_std_20)
ts_warnings(rfq_core)

if(TS_BUILD_ADAPTERS)
  find_package(Threads REQUIRED)
  find_package(nlohmann_json CONFIG REQUIRED)
  find_package(RdKafka CONFIG REQUIRED)
  find_package(hiredis CONFIG REQUIRED)

  add_library(rfq_messaging
    src/messaging/event_codec.cpp
    src/messaging/event_dispatcher.cpp
    src/messaging/kafka_publisher.cpp
    src/store/redis_state_store.cpp)
  target_link_libraries(rfq_messaging
    PUBLIC rfq_core Threads::Threads
    PRIVATE nlohmann_json::nlohmann_json RdKafka::rdkafka++ hiredis::hiredis)
  ts_warnings(rfq_messaging)

  add_executable(engine_bench bench/engine_bench.cpp)
  target_link_libraries(engine_bench PRIVATE rfq_messaging)

  add_executable(rfq_demo src/apps/rfq_demo.cpp)
  target_link_libraries(rfq_demo PRIVATE rfq_messaging)

  find_package(quickfix CONFIG REQUIRED)
  find_file(FIX44_DICTIONARY FIX44.xml PATH_SUFFIXES share/quickfix REQUIRED)

  add_library(rfq_fix src/fix/fix_gateway.cpp)
  target_link_libraries(rfq_fix PUBLIC rfq_core quickfix)
  target_compile_definitions(rfq_fix PUBLIC NOMINMAX WIN32_LEAN_AND_MEAN)

  find_package(prometheus-cpp CONFIG REQUIRED)
  add_library(rfq_metrics src/metrics/prometheus_metrics.cpp)
  target_link_libraries(rfq_metrics PUBLIC rfq_core prometheus-cpp::core)
  target_include_directories(rfq_metrics PUBLIC src)
  ts_warnings(rfq_metrics)

  add_executable(rfq_gateway src/apps/rfq_gateway.cpp)
  target_link_libraries(rfq_gateway PRIVATE rfq_fix rfq_messaging rfq_metrics prometheus-cpp::pull)

  configure_file(config/gateway.cfg.in ${CMAKE_BINARY_DIR}/gateway.cfg @ONLY)
endif()

if(TS_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()
