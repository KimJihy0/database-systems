# Sources
set(DB_SOURCE_DIR src)
set(DB_SOURCES
  ${DB_SOURCE_DIR}/bpt.cc
  ${DB_SOURCE_DIR}/recov.cc
  ${DB_SOURCE_DIR}/trx.cc
  ${DB_SOURCE_DIR}/buffer.cc
  ${DB_SOURCE_DIR}/log.cc
  ${DB_SOURCE_DIR}/file.cc
  )

# Headers
set(DB_HEADER_DIR include)
set(DB_HEADERS
  ${DB_HEADER_DIR}/bpt.h
  ${DB_HEADER_DIR}/recov.h
  ${DB_HEADER_DIR}/trx.h
  ${DB_HEADER_DIR}/buffer.h
  ${DB_HEADER_DIR}/log.h
  ${DB_HEADER_DIR}/file.h
  )

add_library(db STATIC ${DB_HEADERS} ${DB_SOURCES})

target_include_directories(db
  PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/${DB_HEADER_DIR}"
  )

