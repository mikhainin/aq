#pragma once
namespace filter {

struct string_operator {
    enum ops_t {
        CONTAINS
    };
    
    string_operator(ops_t op) : op(op) {
    }

    ops_t op;
};

}