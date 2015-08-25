#include <util/algorithm.h>

#include "detail/ast.hpp"
#include "state.h"

namespace filter {


state::state() {
}

void state::resetState() {
    currentState = false;
    array_states.clear();
}

void state::setState(bool newState) {
    currentState = newState;
}


bool state::getState() const {
    if (!is_array_element) {
        return currentState;
    } else {
        if (array_index == detail::array_element::ALL) {
            return not util::algorithm::contains(
                array_states.begin(), array_states.end(), false
            );
        } else if (array_index == detail::array_element::ANY) {
            return util::algorithm::contains(
                array_states.begin(), array_states.end(), true
            );
        } else if (array_index == detail::array_element::NONE) {
            return not util::algorithm::contains(
                array_states.begin(), array_states.end(), true
            );
        } else {
            return array_states.size() > array_index &&
                        array_states[array_index];
        }
    }
}


void state::pushState() {
    array_states.push_back(currentState);
}
}
