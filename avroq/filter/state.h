#ifndef __filter__state__
#define __filter__state__

#include <vector>

namespace filter {

struct state {

    state();

    bool currentState = false;
    bool is_array_element = false;
    int array_index = 0;
    std::vector<bool> array_states;


    void resetState();
    void setState(bool newState);
    bool getState() const;
    void pushState();

};

}

#endif