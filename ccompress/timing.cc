#include "timing.h"

namespace timing {

    using namespace std;
    using namespace chrono;

    system_clock::time_point time_now() {
        return high_resolution_clock::now();
    }

    void duration(system_clock::time_point start,
                  system_clock::time_point stop, std::string func_name) {
        auto duration = duration_cast<milliseconds>(stop - start);

        cout << "\nOperation ***" << func_name
             << "*** executed in: " << duration.count() << " milliseconds"
             << "\n";
    }

    void duration_ns(system_clock::time_point start,
                     system_clock::time_point stop,
                     std::string func_name) {
        auto duration = duration_cast<nanoseconds>(stop - start);

        cout << "\nOperation ***" << func_name
             << "*** executed in: " << duration.count() << " nanoseconds"
             << "\n";
    }

    void duration_hr(system_clock::time_point start,
                     system_clock::time_point stop,
                     std::string func_name) {
        auto duration = duration_cast<hours>(stop - start);

        cout << "\nOperation ***" << func_name
             << "*** executed in: " << duration.count() << " hours"
             << "\n";
    }
} // namespace timing