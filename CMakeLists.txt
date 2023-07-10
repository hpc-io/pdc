# CMakeLists files in this project can
# refer to the root source directory of the project as ${PDC_SOURCE_DIR} and
# to the root binary directory of the project as ${PDC_BINARY_DIR}.
cmake_minimum_required (VERSION 3.0)

# Setup cmake policies.
foreach(p
    CMP0012 # CMake 2.6; Full Path Libraries; When set to NEW, the full path is included when specifying a link library in the target_link_libraries command.
    CMP0013 # CMake 2.6; Quoted Argument Expansion; When set to NEW, variables within quoted arguments are expanded, allowing variable substitution.
    CMP0014 # CMake 2.8; Relative Path Libraries; When set to NEW, relative paths in link libraries are interpreted as relative to the current source directory.
    CMP0022 # CMake 2.8.12; Same SONAME Overwrite; When set to NEW, shared libraries with the same SONAME are not overwritten during installation.
    CMP0025 # CMake 3.0; Empty if Statement; When set to NEW, an empty if condition is treated as false, providing better control over conditionals.
    CMP0053 # CMake 3.1; Find Package Version; When set to NEW, the VERSION option in find_package is interpreted as a version requirement rather than a version hint.
    CMP0054 # CMake 3.1; Quoted Empty String; When set to NEW, a quoted empty string is treated as false in the if() command.
    CMP0074 # CMake 3.12; Imported Target Usage Requirements; When set to NEW, the "PRIVATE" and "PUBLIC" keywords in target_link_libraries are treated as usage requirements for imported targets.
    CMP0075 # CMake 3.12; GNU-Style Installation Paths on macOS; When set to NEW, the "GNUInstallDirs" module is enabled by default on macOS for "GNU-style" installation paths.
    CMP0083 # CMake 3.14; Imported Targets Visibility; When set to NEW, imported targets created by find_package with namespaced aliases are visible to other targets within the same directory scope.
    CMP0093 # CMake 3.15; Compiler and Linker Flags Handling; When set to NEW, each option in a list of compiler and linker flags is interpreted separately, allowing better control over individual options.
  )
  if(POLICY ${p})
    cmake_policy(SET ${p} NEW)
  endif()
endforeach()

# Set a consistent MACOSX_RPATH default across all CMake versions.
# checks if the variable CMAKE_MACOSX_RPATH is not defined and sets it to 0 if it is not defined.
# The CMAKE_MACOSX_RPATH variable is used to control the behavior of the macOS-specific RPATH feature in CMake. 
# RPATH is a linker option that specifies the runtime library search path for executables and shared libraries.
# By default, when CMAKE_MACOSX_RPATH is not defined, the code sets it to 0, indicating that the project does not use the macOS RPATH feature. 
# This means that the runtime library search path will not be embedded in the executables or shared libraries produced by the project.
# This code snippet ensures a consistent default value for CMAKE_MACOSX_RPATH across different configurations of the project, even if the variable is not explicitly set before.
# When CMake 2.8.12 is required, change this default to 1.
# When CMake 3.0.0 is required, remove this block (see CMP0042).
if(NOT DEFINED CMAKE_MACOSX_RPATH)
  set(CMAKE_MACOSX_RPATH 0)
endif()

# Project Name : PDC, Programming Language : C
project(PDC C)

# CMake will search for any header files under the project root directory.
include_directories(
  ${PROJECT_SOURCE_DIR}
)

# Check if MPI_RUN_CMD is not defind. If not, set it to mpiexec.
if(NOT MPI_RUN_CMD)
    set(MPI_RUN_CMD mpiexec)
endif()

#------------------------------------------------------------------------------
# Version information
#------------------------------------------------------------------------------
set(PDC_VERSION_MAJOR "0")
set(PDC_VERSION_MINOR "1")
set(PDC_VERSION_PATCH "0")
set(PDC_PACKAGE "pdc")
set(PDC_PACKAGE_NAME "PDC")
set(PDC_PACKAGE_VERSION "${PDC_VERSION_MAJOR}.${PDC_VERSION_MINOR}.${PDC_VERSION_PATCH}")
set(PDC_PACKAGE_VERSION_MAJOR "${PDC_VERSION_MAJOR}.${PDC_VERSION_MINOR}")
set(PDC_PACKAGE_VERSION_MINOR "${PDC_VERSION_PATCH}")
set(PDC_PACKAGE_STRING "${PDC_PACKAGE_NAME} ${PDC_PACKAGE_VERSION}")
set(PDC_PACKAGE_TARNAME "${PDC_PACKAGE}")


#------------------------------------------------------------------------------
# Setup CMake Prefix Paths for searching external libraries.
#------------------------------------------------------------------------------
if(NOT CMAKE_PREFIX_PATH)
  set(CMAKE_PREFIX_PATH ${CMAKE_INSTALL_PREFIX})
endif()
set(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH} $ENV{MERCURY_DIR})

#------------------------------------------------------------------------------
# Setup install and output Directories
#------------------------------------------------------------------------------
if(NOT CMAKE_INSTALL_PREFIX)
  set(CMAKE_INSTALL_PREFIX ${PROJECT_BINARY_DIR})
endif()
if(NOT PDC_INSTALL_BIN_DIR)
  set(PDC_INSTALL_BIN_DIR ${CMAKE_INSTALL_PREFIX}/bin)
endif()
if(NOT PDC_INSTALL_LIB_DIR)
  set(PDC_INSTALL_LIB_DIR ${CMAKE_INSTALL_PREFIX}/lib)
endif()
if(NOT PDC_INSTALL_INCLUDE_DIR)
  set(PDC_INSTALL_INCLUDE_DIR ${CMAKE_INSTALL_PREFIX}/include)
endif()
if(NOT PDC_INSTALL_DATA_DIR)
  set(PDC_INSTALL_DATA_DIR ${CMAKE_INSTALL_PREFIX}/share)
endif()


# The purpose of setting CMAKE_INSTALL_RPATH_USE_LINK_PATH to TRUE is to 
# include the directories specified by the linker during the build process 
# in the install RPATH, ensuring that the installed binary can locate its 
# required shared libraries during runtime, regardless of the platform being used.
#
# Setting this ensures that "make install" will leave rpaths to external
# libraries intact on "make install". This ensures that one can install a
# version of PDC on the build machine without any issues. If this not
# desired, simply specify CMAKE_INSTALL_RPATH_USE_LINK_PATH when configuring
# PDC and "make install" will strip all rpaths, which is default behavior.
if(NOT CMAKE_INSTALL_RPATH_USE_LINK_PATH)
  set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)
endif()

#------------------------------------------------------------------------------
# Setup CMake Environment
#------------------------------------------------------------------------------
if(APPLE)
  # We are doing a unix-style install i.e. everything will be installed in
  # CMAKE_INSTALL_PREFIX/bin and CMAKE_INSTALL_PREFIX/lib etc. as on other unix
  # platforms. We still need to setup CMAKE_INSTALL_NAME_DIR correctly so that
  # the binaries point to appropriate location for the libraries.

  # 1. Make CMAKE_INSTALL_PREFIX publicly accessible, if it was hidden in
  #    previous pass
  get_property(is_internal CACHE CMAKE_INSTALL_PREFIX PROPERTY TYPE)
  if(is_internal STREQUAL "INTERNAL")
    set(CMAKE_INSTALL_PREFIX ${CACHED_CMAKE_INSTALL_PREFIX} CACHE PATH "Install prefix" FORCE)
  else()
    set(CMAKE_INSTALL_PREFIX ${CACHED_CMAKE_INSTALL_PREFIX} CACHE PATH "Install prefix")
  endif()
  unset(MACOSX_APP_INSTALL_PREFIX CACHE)

  set(CMAKE_INSTALL_NAME_DIR "@rpath")
  mark_as_advanced(
    CMAKE_OSX_ARCHITECTURES
    CMAKE_OSX_DEPLOYMENT_TARGET
    CMAKE_OSX_SYSROOT
    )
endif()

#------------------------------------------------------------------------------
if(NOT PDC_EXTERNALLY_CONFIGURED)
  set(CMAKE_RUNTIME_OUTPUT_DIRECTORY
      ${PROJECT_BINARY_DIR}/bin CACHE PATH "Single Directory for all Executables."
  )
  set(EXECUTABLE_OUTPUT_PATH ${CMAKE_RUNTIME_OUTPUT_DIRECTORY})
  set(CMAKE_LIBRARY_OUTPUT_DIRECTORY
      ${PROJECT_BINARY_DIR}/bin CACHE PATH "Single Directory for all Libraries"
  )
  set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY
      ${PROJECT_BINARY_DIR}/bin CACHE PATH "Single Directory for all static libraries."
  )
endif()
set(PDC_CMAKE_DIR "${PDC_SOURCE_DIR}/CMake")
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${PDC_CMAKE_DIR})

#------------------------------------------------------------------------------
# Disallow in-source build
#------------------------------------------------------------------------------
if("${PDC_SOURCE_DIR}" STREQUAL "${PDC_BINARY_DIR}")
  message(FATAL_ERROR
    "PDC requires an out of source Build. "
    "Please create a separate binary directory and run CMake there.")
endif()

#------------------------------------------------------------------------------
# Set whether or not to disable compiler warnings
#------------------------------------------------------------------------------
set(SUPPRESSED_LIST "")
# Disable warnings for unused parameters
option(PDC_SUPPRESS_UNUSED_ARG "Disable unused argument warnings" ON)
if(PDC_SUPPRESS_UNUSED_ARG)
  set(SUPPRESSED_LIST "-Wno-unused-parameter" ${SUPPRESSED_LIST})
endif()

# Disable warnings for unused variables
option(PDC_SUPPRESS_UNUSED_VAR "Disable unused variable warnings" ON)
if(PDC_SUPPRESS_UNUSED_VAR)
  set(SUPPRESSED_LIST "-Wno-unused-variable" ${SUPPRESSED_LIST})
endif()

# Disable warnings for unused functions
option(PDC_SUPPRESS_UNUSED_FUNC "Disable unused function warnings" ON)
if(PDC_SUPPRESS_UNUSED_FUNC)
  set(SUPPRESSED_LIST "-Wno-unused-function" ${SUPPRESSED_LIST})
endif()

# Disable warnings for -Wunused-result
option(PDC_SUPPRESS_UNUSED_RESULT "Disable unused result warnings" ON)
if(PDC_SUPPRESS_UNUSED_RESULT)
  set(SUPPRESSED_LIST "-Wno-unused-result" ${SUPPRESSED_LIST})
endif()

# Disable warnings for -Wmaybe-uninitialized
option(PDC_SUPPRESS_MAYBE_UNINIT "Disable maybe-uninitialized warnings" ON)
if(PDC_SUPPRESS_MAYBE_UNINIT)
  if(NOT APPLE)
    set(SUPPRESSED_LIST "-Wno-maybe-uninitialized" ${SUPPRESSED_LIST})
  endif()
endif()

# Disable warnings for -Wunused-but-set-variable
option(PDC_SUPPRESS_UNUSED_BUT_SET "Disable unused-but-set-variable warnings" ON)
if(PDC_SUPPRESS_UNUSED_BUT_SET)
  set(SUPPRESSED_LIST "-Wno-unused-but-set-variable" ${SUPPRESSED_LIST})
endif()

# Disable warnings for -Wsign-compare
option(PDC_SUPPRESS_SIGN_COMPARE "Disable sign-compare warnings" ON)
if(PDC_SUPPRESS_SIGN_COMPARE)
  set(SUPPRESSED_LIST "-Wno-sign-compare" ${SUPPRESSED_LIST})
endif()

# Disable warnings for -Wformat
option(PDC_SUPPRESS_FORMAT "Disable format warnings" ON)
if(PDC_SUPPRESS_FORMAT)
  set(SUPPRESSED_LIST "-Wno-format" ${SUPPRESSED_LIST})
endif()

# Disable warnings for -Wcast-qual
option(PDC_SUPPRESS_CAST_QUAL "Disable cast-qual warnings" ON)
if(PDC_SUPPRESS_CAST_QUAL)
  set(SUPPRESSED_LIST "-Wno-cast-qual" ${SUPPRESSED_LIST})
endif()

# Disable warnings for -Wdeprecated-non-prototype if on Apple
if(APPLE)
  set(SUPPRESSED_LIST "-Wno-deprecated-non-prototype" ${SUPPRESSED_LIST})
endif()

#------------------------------------------------------------------------------
# Set a default build type if none was specified
#------------------------------------------------------------------------------
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  message(STATUS "Setting build type to 'RelWithDebInfo' as none was specified.")
  set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "Choose the type of build." FORCE)
  # Set the possible values of build type for cmake-gui
  set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release"
    "MinSizeRel" "RelWithDebInfo")
endif()

if(NOT CMAKE_C_FLAGS AND CMAKE_COMPILER_IS_GNUCC)
  message(STATUS "GCC detected, setting additional flags")
  set(CMAKE_C_FLAGS "-Wall -Wextra -Winline -Wcast-qual -std=gnu99 -Wshadow -fcommon" CACHE STRING "Flags used by the compiler during all build types." FORCE)
endif()

#-----------------------------------------------------------------------------
# Targets built within this project are exported at Install time for use
# by other projects.
#-----------------------------------------------------------------------------
if(NOT PDC_EXPORTED_TARGETS)
  set(PDC_EXPORTED_TARGETS "pdc-targets")
endif()

#------------------------------------------------------------------------------
# Choose static or shared libraries.
#------------------------------------------------------------------------------
option(BUILD_SHARED_LIBS "Build with shared libraries." OFF)

#-------------------------------------------------------------------------------
function(pdc_set_lib_options libtarget libname libtype)
  if(${libtype} MATCHES "SHARED")
    if(WIN32 AND NOT MINGW)
      set(LIB_RELEASE_NAME "${libname}")
      set(LIB_DEBUG_NAME "${libname}_D")
      set(LIB_VERSION ${PDC_PACKAGE_VERSION_MAJOR})
    else()
      set(LIB_RELEASE_NAME "${libname}")
      set(LIB_DEBUG_NAME "${libname}_debug")
      set(LIB_VERSION ${PDC_PACKAGE_VERSION})
    endif()
  else()
    if(WIN32 AND NOT MINGW)
      set(LIB_RELEASE_NAME "lib${libname}")
      set(LIB_DEBUG_NAME "lib${libname}_D")
    else()
      # if the generator supports configuration types or if the CMAKE_BUILD_TYPE has a value
      if(CMAKE_CONFIGURATION_TYPES OR CMAKE_BUILD_TYPE)
        set(LIB_RELEASE_NAME "${libname}")
        set(LIB_DEBUG_NAME "${libname}_debug")
      else()
        set(LIB_RELEASE_NAME "lib${libname}")
        set(LIB_DEBUG_NAME "lib${libname}_debug")
      endif()
    endif()
  endif()

  set_target_properties(${libtarget}
      PROPERTIES
      DEBUG_OUTPUT_NAME          ${LIB_DEBUG_NAME}
      RELEASE_OUTPUT_NAME        ${LIB_RELEASE_NAME}
      MINSIZEREL_OUTPUT_NAME     ${LIB_RELEASE_NAME}
      RELWITHDEBINFO_OUTPUT_NAME ${LIB_RELEASE_NAME}
      VERSION                    ${LIB_VERSION}
      SOVERSION                  ${LIB_VERSION}
  )

  #----- Use MSVC Naming conventions for Shared Libraries
  if(MINGW AND ${libtype} MATCHES "SHARED")
    set_target_properties(${libtarget}
        PROPERTIES
        IMPORT_SUFFIX ".lib"
        IMPORT_PREFIX ""
        PREFIX ""
    )
  endif()
endfunction()

#--------------- start generating config.h from config.h.cmake ---------------

#-----------------------------------------------------------------------------
# Server mode option
#-----------------------------------------------------------------------------
option(PDC_USE_SHARED_SERVER
    "Use shared server with client mode." OFF)
if(PDC_USE_SHARED_SERVER)
  set(PDC_HAS_SHARED_SERVER 1)
endif()

#-----------------------------------------------------------------------------
# DRC option
#-----------------------------------------------------------------------------
option(PDC_USE_CRAY_DRC
    "Use Cray DRC to allow multi-job communication." OFF)
if(PDC_USE_CRAY_DRC)
  find_package(DRC REQUIRED)
  if(DRC_FOUND)
    set(PDC_HAS_CRAY_DRC 1)
    include_directories(${DRC_INCLUDE_DIRS})
    set(PDC_EXT_LIB_DEPENDENCIES
      ${PDC_EXT_LIB_DEPENDENCIES}
      ${DRC_LIBRARIES}
    )
  else()
    message(FATAL_ERROR "Could not find Cray DRC.")
  endif()
endif()

#-----------------------------------------------------------------------------
# MPI option
#-----------------------------------------------------------------------------
option(PDC_ENABLE_MPI "Enable MPI." ON)
if(PDC_ENABLE_MPI)
  find_package(MPI)
  if(MPI_FOUND)
    set(ENABLE_MPI 1)
  endif()
endif()

#-----------------------------------------------------------------------------
# LUSTRE option
#-----------------------------------------------------------------------------
option(PDC_ENABLE_LUSTRE "Enable Lustre." OFF)
if(PDC_ENABLE_LUSTRE)
  set(ENABLE_LUSTRE 1)
  set(PDC_LUSTRE_TOTAL_OST "256" CACHE STRING "Number of Lustre OSTs")
endif()

#-----------------------------------------------------------------------------
# TIMING option
#-----------------------------------------------------------------------------
option(PDC_TIMING "Enable timing." OFF)
if(PDC_TIMING)
  set(PDC_TIMING 1)
endif()

#-----------------------------------------------------------------------------
# SERVER CACHE option
#-----------------------------------------------------------------------------
option(PDC_SERVER_CACHE "Enable timing." OFF)
if(PDC_SERVER_CACHE)
  set(PDC_SERVER_CACHE 1)
endif()


#-----------------------------------------------------------------------------
# CHECKPOINT option
#-----------------------------------------------------------------------------
option(PDC_ENABLE_CHECKPOINT "Enable checkpointing." ON)
if(PDC_ENABLE_CHECKPOINT)
  set(PDC_ENABLE_CHECKPOINT 1)
endif()

#-----------------------------------------------------------------------------
# Close server by application option
#-----------------------------------------------------------------------------
option(PDC_ENABLE_APP_CLOSE_SERVER
    "Close pdc server at the end of application." OFF)
if(PDC_ENABLE_APP_CLOSE_SERVER)
  set(ENABLE_APP_CLOSE_SERVER 1)
endif()

#-----------------------------------------------------------------------------
# 
#-----------------------------------------------------------------------------
option(PDC_ENABLE_WAIT_DATA 
    "Wait for data fanalized in FS when object unmap is called." OFF)
if(PDC_ENABLE_WAIT_DATA)
  set(ENABLE_WAIT_DATA 1)
endif()

if(PDC_ENABLE_APP_CLOSE_SERVER AND NOT PDC_ENABLE_WAIT_DATA) 
  message("WARNING: data tranx may be not done when closing server by application")
  message("Wait for data, or close the server later")
endif()

#-----------------------------------------------------------------------------
# MULTITHREAD option
#-----------------------------------------------------------------------------
option(PDC_ENABLE_MULTITHREAD "Enable multithreading." OFF)
if(PDC_ENABLE_MULTITHREAD)
  set(ENABLE_MULTITHREAD 1)
endif()

#-----------------------------------------------------------------------------
# Pthread support on Mac OS X
#-----------------------------------------------------------------------------
if(APPLE)
  find_package(Threads REQUIRED)
endif()

#-----------------------------------------------------------------------------
# PROFILING option
#-----------------------------------------------------------------------------
option(PDC_ENABLE_PROFILING "Enable profiling." OFF)
if(PDC_ENABLE_PROFILING)
  set(ENABLE_PROFILING 1)
endif()

#-----------------------------------------------------------------------------
# CACHE option
#-----------------------------------------------------------------------------
option(PDC_SERVER_CACHE "Enable server data caching with pthread." OFF)
if(PDC_SERVER_CACHE)
    set(PDC_SERVER_CACHE 1)
endif()

# Not used
# option(PDC_ENABLE_CACHE "Enable data caching." OFF)
# if(PDC_ENABLE_CACHE)
#   set(ENABLE_CACHE 1)
# endif()

#-----------------------------------------------------------------------------
# ATTRIBUTE option
#-----------------------------------------------------------------------------
option(PDC_HAVE_ATTRIBUTE_UNUSED "Use compiler attribute" ON)
if(PDC_HAVE_ATTRIBUTE_UNUSED)
  set(HAVE_ATTRIBUTE 1)
endif()

#-----------------------------------------------------------------------------
# Query with Fastbit option
#-----------------------------------------------------------------------------
option(PDC_ENABLE_FASTBIT "Enable Fastbit." OFF)
if(PDC_ENABLE_FASTBIT)
    find_library(FASTBIT fastbit)
    set(ENABLE_FASTBIT 1)
endif()

#------------------------------------------------------------------------------
# Data type
#------------------------------------------------------------------------------
include(CheckTypeSize)
check_type_size("float" VAR_SIZE_FLOAT)
check_type_size("double" VAR_SIZE_DOUBLE)
check_type_size("char" VAR_SIZE_CHAR)
check_type_size("short" VAR_SIZE_SINT)
check_type_size("int" VAR_SIZE_INT)
check_type_size("long" VAR_SIZE_LINT)
check_type_size("long long" VAR_SIZE_LLINT)
check_type_size("unsigned" VAR_SIZE_UINT)
check_type_size("unsigned long" VAR_SIZE_ULINT)
check_type_size("unsigned long long" VAR_SIZE_ULLINT)
check_type_size("int64_t" VAR_SIZE_64INT)
check_type_size("uint64_t" VAR_SIZE_U64INT)
 
CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/pdc_config_sys.h.cmake ${PROJECT_BINARY_DIR}/pdc_config_sys.h)

configure_file(
  ${CMAKE_CURRENT_SOURCE_DIR}/pdc_config.h.in
  ${PROJECT_BINARY_DIR}/pdc_config.h
)

#---------------------- end of generating config.h ---------------------------

#-----------------------------------------------------------------------------
# Source
#-----------------------------------------------------------------------------
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/src/commons)
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/src/api)
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/src/server)



#-----------------------------------------------------------------------------
# Testing
#-----------------------------------------------------------------------------
option(BUILD_TESTING "Build testing." OFF)
option(BUILD_MPI_TESTING "Build MPI testing." OFF)
if(NOT PDC_EXTERNALLY_CONFIGURED AND BUILD_TESTING)
  enable_testing()
  include(CTest)
  add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/src/tests)
endif()

#-----------------------------------------------------------------------------
# Build doxygen documentation.
#-----------------------------------------------------------------------------
# option(BUILD_DOCUMENTATION "Build documentation." OFF)
# if(BUILD_DOCUMENTATION)
#   add_subdirectory(doc/doxygen)
# endif()

#-----------------------------------------------------------------------------
# Configure the config.cmake file for the build directory
#-----------------------------------------------------------------------------
configure_file(
  ${PDC_SOURCE_DIR}/CMake/pdc-config.cmake.build.in
  ${PDC_BINARY_DIR}/pdc-config.cmake @ONLY
)

#-----------------------------------------------------------------------------
# Configure the config.cmake file for the install directory
#-----------------------------------------------------------------------------
configure_file(
  ${PDC_SOURCE_DIR}/CMake/pdc-config.cmake.install.in
  ${PDC_BINARY_DIR}/CMakeFiles/pdc-config.cmake @ONLY
)

install(
  FILES
    ${PDC_BINARY_DIR}/CMakeFiles/pdc-config.cmake
  DESTINATION
    ${PDC_INSTALL_DATA_DIR}/cmake/pdc
)

#-----------------------------------------------------------------------------
# Configure the pdc-config-version .cmake file for the install directory
#-----------------------------------------------------------------------------
configure_file(
  ${PDC_SOURCE_DIR}/CMake/pdc-config-version.cmake.in
  ${PDC_BINARY_DIR}/CMakeFiles/pdc-config-version.cmake @ONLY
)



install(
  FILES
    ${PDC_BINARY_DIR}/CMakeFiles/pdc-config-version.cmake
  DESTINATION
    ${PDC_INSTALL_DATA_DIR}/cmake/pdc
)

install(
  DIRECTORY
    ${PDC_BINARY_DIR}/bin
  DESTINATION
    ${PDC_INSTALL_DATA_DIR}/test
  FILES_MATCHING PATTERN "*"
  PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
)


install(
  DIRECTORY
    ${PDC_SOURCE_DIR}/scripts
  DESTINATION
    ${CMAKE_INSTALL_PREFIX}
  FILES_MATCHING PATTERN "*.sh"
  PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
)


#install(
#  FILES
#    ${PDC_BINARY_DIR}/bin/pdc_server.exe
#    ${PDC_BINARY_DIR}/bin/close_server
#  DESTINATION
#    ${CMAKE_INSTALL_PREFIX}/bin
#)

#-----------------------------------------------------------------------------
# CPack
#-----------------------------------------------------------------------------
if(NOT PDC_EXTERNALLY_CONFIGURED)
# set(CPACK_PACKAGE_DESCRIPTION_FILE ${PDC_SOURCE_DIR}/README.md)
  set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "Proactive Data Containers")
  set(CPACK_PACKAGE_NAME "PDC")
  set(CPACK_PACKAGE_VENDOR "LBNL / The HDF Group / ANL")
  set(CPACK_PACKAGE_VERSION_MAJOR ${PDC_VERSION_MAJOR})
  set(CPACK_PACKAGE_VERSION_MINOR ${PDC_VERSION_MINOR})
  set(CPACK_PACKAGE_VERSION_PATCH ${PDC_VERSION_PATCH})
  set(CPACK_GENERATOR "TBZ2")
  set(CPACK_SOURCE_PACKAGE_FILE_NAME ${PDC_PACKAGE_TARNAME}-${PDC_PACKAGE_VERSION})
  set(CPACK_SOURCE_IGNORE_FILES ".git*;/GitSetup/;/.git/;.swp$;.#;/#;.*~")
  set(CPACK_SOURCE_STRIP_FILES "")
  include(CPack)
endif()

add_custom_target(format
    COMMAND find
    ${PDC_SOURCE_DIR}/src/api
    ${PDC_SOURCE_DIR}/src/server
    ${PDC_SOURCE_DIR}/src/tests
    -type f
    (
    -name "*.c"
    -o
    -name "*.h"
    )
    -print0
  | xargs -0 clang-format -i -style=file && echo "... done"
  COMMENT "clang-format all source codes"
  VERBATIM 
)

