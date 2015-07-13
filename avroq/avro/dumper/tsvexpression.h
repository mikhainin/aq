#pragma once

#include <unordered_map>

namespace avro {
namespace dumper {

struct TsvExpression {
	std::unordered_map<int, int> what;
	int pos = 0;
};

}
}