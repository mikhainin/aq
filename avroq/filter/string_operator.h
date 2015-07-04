#pragma once
namespace filter {

struct string_operator {
    enum ops_t {
        CONTAINS,
        STARTS_WITH,
        ENDS_WITH
    };
    
    string_operator(ops_t op) : op(op) {
    }

    ops_t op;
};

}