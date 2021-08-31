/**
 * This is a sample source file corresponding to a public header file.
 *
 * Copyright information goes here
 */

#include <project/Public.hpp>

#include "Private.hpp"

namespace eugene {
    bool library_works() {
        return eugene_internal::library_works();
    }
}
