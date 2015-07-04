#pragma once

namespace avro {
namespace dumper {

struct TsvExpression {
	std::map<int, int> what;
	int pos = 0;
};

}
}