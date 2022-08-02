#include "hepnos/Statistics.hpp"
#include <thallium.hpp>

namespace hepnos {

double wtime() {
    return thallium::timer::wtime();
}

}
