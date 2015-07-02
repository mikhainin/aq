#include "equality_expression.h"


namespace filter {

equality_expression::equality_expression() {
}

equality_expression::equality_expression(const std::string &ident) :
    identifier(ident) {
}

equality_expression& equality_expression::operator == (const type &constant) {
    this->constant = constant;
    op = EQ;
    return *this;
}

equality_expression& equality_expression::operator != (const type &constant) {
    this->constant = constant;
    op = NE;
    return *this;
}

void equality_expression::resetState() {
    state = false;
}

void equality_expression::setState(bool newState) {
    state = newState;
}

}
