#ifndef __util_onscopeexit_
#define __util_onscopeexit_

#include <functional>

namespace util {

    struct on_scope_exit {
    	on_scope_exit(std::function<void()> f) : f(f ) {}
        ~on_scope_exit() { f(); }
    private:
        std::function<void()> f;
    };

}

#endif
