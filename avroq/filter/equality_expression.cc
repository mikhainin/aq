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


void equality_expression::resetState() {
    state = false;
}

void equality_expression::setState(bool newState) {
    state = newState;
}


}
