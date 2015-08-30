#include <util/algorithm.h>
#include "detail/ast.hpp"

#include "record_expression.h"
#include "equality_expression.h"


namespace filter {

equality_expression::equality_expression() : state() {
}

equality_expression::equality_expression(const std::string &ident) :
    state(),
    identifier(ident) {
}

equality_expression::equality_expression(const detail::array_element &ident) :
    state(),
    identifier(ident.identifier) {

    is_array_element = true;
    array_index = ident.index;
}

equality_expression& equality_expression::operator == (const type &constant) {
    if (constant.type() == typeid(filter::nil)) {
        op = IS_NIL;
    } else {
        this->constant = constant;
        op = EQ;
    }
    return *this;
}

equality_expression& equality_expression::operator != (const type &constant) {
    if (constant.type() == typeid(filter::nil)) {
        op = NOT_NIL;
    } else {
        this->constant = constant;
        op = NE;
    }
    return *this;
}

equality_expression& equality_expression::operator < (const type &constant) {
    this->constant = constant;
    op = LT;
    return *this;
}

equality_expression& equality_expression::operator > (const type &constant) {
    this->constant = constant;
    op = GT;
    return *this;
}

equality_expression& equality_expression::operator <= (const type &constant) {
    this->constant = constant;
    op = LE;
    return *this;
}


equality_expression& equality_expression::operator >= (const type &constant) {
    this->constant = constant;
    op = GE;
    return *this;
}



equality_expression& equality_expression::operator |= (const string_operator &strop) {
    op = STRING;
    this->strop = strop.op;
    return *this;
}

equality_expression& equality_expression::operator |= (const type &constant) {
    this->constant = constant;
    return *this;
}

/*
void equality_expression::resetState() {
    state = false;
    array_states.clear();
}

void equality_expression::setState(bool newState) {
    state = newState;
}


bool equality_expression::getState() const {
    if (!is_array_element) {
        return state;
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


void equality_expression::pushState() {
    array_states.push_back(state);
}
*/
void equality_expression::setParent(record_expression * parent) {

    assert(parent && "parent cannot be null");
    this->parent = parent;
    // identifier = parent->identifier + '.' + identifier;

}


}
