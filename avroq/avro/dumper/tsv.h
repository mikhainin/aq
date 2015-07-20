#pragma once

#include <ostream>
#include <sstream>
#include <map>

#include <avro/node/all_nodes.h>
#include <avro/stringbuffer.h>

#include "tsvexpression.h"

namespace avro {
namespace dumper {



class Dumper {
public:
    virtual ~Dumper() {}
    virtual void dump(std::ostream &os) = 0;
};

template<typename T>
class TDumper : public Dumper {
public:

    TDumper(const T &t) : Dumper(), t(t) {
    }
    void dump(std::ostream &os) {
        os << t;
    }
private:
    T t;
};

template<>
class TDumper<bool> : public Dumper {
public:

    TDumper(const bool &t) : Dumper(), t(t) {
    }   
    void dump(std::ostream &os) {
        os << (t ? "true" : "false");
    }   
private:
    bool t;
};

class Tsv {

public:
    Tsv(const TsvExpression &wd) : whatDump(wd) {
    	toDump.resize(whatDump.pos);
    }

    template<typename T, typename NodeType>
    void addIfNecessary(const T &t, const NodeType &n) {
    	auto p = whatDump.what.find(n.getNumber());
    	if (p != whatDump.what.end()) {
            toDump[p->second].reset(new TDumper<T>(t));
    	}
    }

    template<typename T>
    void addToPosition(const T &t, int position) {
        toDump[position].reset(new TDumper<T>(t));
    }

    void String(const StringBuffer &s, const node::String &n) {
    	addIfNecessary(s, n);
    }

    void MapName(const StringBuffer &name) {
    }

    void MapValue(const StringBuffer &s, const node::String &n) {
    }
    void MapValue(int i, const node::Int &n) {
    }

    void Int(int i, const node::Int &n) {
    	addIfNecessary(i, n);
    }

    void Long(long l, const node::Long &n) {
    	addIfNecessary(l, n);
    }

    void Float(float f, const node::Float &n) {
    	addIfNecessary(f, n);
    }

    void Double(double d, const node::Double &n) {
    	addIfNecessary(d, n);
    }

    void Boolean(bool b, const node::Boolean &n) {
    	addIfNecessary(b, n);
    }

    void Null(const node::Null &n) {
    	addIfNecessary(std::string("null"), n);
    }

    void Union(int index, const node::Union &n) {
    }

    void RecordBegin(const node::Record &r) {
    }

    void RecordEnd(const node::Record &r) {
    }

    void ArrayBegin(const node::Array &a) {
    }

    void ArrayEnd(const node::Array &a) {
    }

    void CustomBegin(const node::Custom &c) {
    }

    void Enum(const node::Enum &e, int index) {
    	addIfNecessary(e[index], e);
    }

    void MapBegin(const node::Map &m) {
    }

    void MapEnd(const node::Map &m) {
    }

    void EndDocument(std::function<void(const std::string &)> dumpMethod) {
    	auto p = toDump.begin();
    	(*p)->dump(outStream);
    	++p;
    	while(p != toDump.end()) {
    		outStream << whatDump.fieldSeparator;
    		(*p)->dump(outStream);
    		++p;
    	}
        const auto &str = outStream.str();
        dumpMethod(str);
    }

private:
    std::vector<std::unique_ptr<Dumper>> toDump;
    const TsvExpression &whatDump;
    std::ostringstream outStream;
};




}
}
