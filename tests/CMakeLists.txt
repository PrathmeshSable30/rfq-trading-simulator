include(FetchContent)

FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/refs/tags/v1.15.2.zip
)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

include(GoogleTest)

add_executable(unit_tests unit/rfq_engine_test.cpp)
target_link_libraries(unit_tests PRIVATE rfq_core GTest::gtest_main)
gtest_discover_tests(unit_tests)

if(TS_BUILD_ADAPTERS)
  add_executable(messaging_tests
    unit/event_codec_test.cpp
    unit/event_dispatcher_test.cpp)
  target_link_libraries(messaging_tests PRIVATE rfq_messaging GTest::gtest_main)
  gtest_discover_tests(messaging_tests)

  add_executable(fix_tests unit/fix_gateway_test.cpp)
  target_link_libraries(fix_tests PRIVATE rfq_fix GTest::gtest_main)
  gtest_discover_tests(fix_tests)

  add_executable(metrics_tests unit/prometheus_metrics_test.cpp)
  target_link_libraries(metrics_tests PRIVATE rfq_metrics GTest::gtest_main)
  gtest_discover_tests(metrics_tests)

  # Skip themselves unless RFQ_REDIS_HOST / RFQ_KAFKA_BROKERS point at the compose stack.
  add_executable(integration_tests
    integration/redis_state_store_test.cpp
    integration/kafka_publisher_test.cpp)
  target_link_libraries(integration_tests PRIVATE rfq_messaging hiredis::hiredis GTest::gtest_main)
  gtest_discover_tests(integration_tests PROPERTIES LABELS integration)
endif()
