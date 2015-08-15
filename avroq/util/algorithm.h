#ifndef __util_algorhythm__
#define __util_algorhythm__


#include <algorithm>

namespace util {
namespace algorithm {


template <class Iter>
bool contains(const Iter &start, const Iter &end, const typename Iter::value_type &v) {
    return std::find(start, end, v) != end;
}



}
}

#endif