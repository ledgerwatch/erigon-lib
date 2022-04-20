#pragma once

#include <chrono>
#include <iostream>
#include <string>

namespace timing {

    using namespace std;
    using namespace chrono;

    system_clock::time_point time_now();

    void duration(system_clock::time_point start,
                  system_clock::time_point stop, std::string func_name);

    void duration_ns(system_clock::time_point start,
                     system_clock::time_point stop,
                     std::string func_name);

    void duration_hr(system_clock::time_point start,
                     system_clock::time_point stop,
                     std::string func_name);
} // namespace timing