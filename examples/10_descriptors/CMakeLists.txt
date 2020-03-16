# Little trick to name the executable the same as the directory that contains it
get_filename_component(THISDIR ${CMAKE_CURRENT_LIST_FILE}/.. ABSOLUTE)
get_filename_component(EXECNAME ${THISDIR} NAME)

# Fin all cpp files in the current directory
file(GLOB SOURCES
    "*.hpp"
    "*.cpp"
)

add_executable(${EXECNAME} ${SOURCES})
target_link_libraries(${EXECNAME} hepnos)
