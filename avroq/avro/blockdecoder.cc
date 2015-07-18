
#include <filter/filter.h>

#include "dumper/tsv.h"
#include "dumper/fool.h"

#include "node/all_nodes.h"
#include "node/nodebypath.h"

#include "predicate/predicate.h"

#include "block.h"
#include "exception.h"
#include "finished.h"
#include "header.h"
#include "typeparser.h"
#include "limiter.h"

#include "blockdecoder.h"


namespace avro {

BlockDecoder::BlockDecoder(const struct header &header, Limiter &limit) : header(header), limit(limit) {
}

namespace {

    struct TypeName {
        TypeName(const std::string &name)
            : name(name) {
        }

        std::string name;
    };

    template <typename T>
    struct ThowTypeName {
        using result_type = T;

        T operator() (double) const {
            throw TypeName("double");
        }
        T operator() (float) const {
            throw TypeName("float");
        }
        T operator() (int) const {
            throw TypeName("int");
        }
        T operator() (const std::string &) const {
            throw TypeName("string");
        }

        T operator() (bool) const {
            throw TypeName("bool");
        }
        template <typename AgrgType>
        T operator() (const AgrgType&) const {
            throw TypeName(std::string("Unknown type:") + typeid(AgrgType).name());
        }
    };

    template <>
    double ThowTypeName<double>::operator() (double d) const {
        return d;
    }
    template <>
    double ThowTypeName<double>::operator() (float f) const {
        return f;
    }
    template <>
    double ThowTypeName<double>::operator() (int i) const {
        return i;
    }

    using ToDouble = ThowTypeName<double>;


    template <>
    float ThowTypeName<float>::operator() (double d) const {
        return d;
    }
    template <>
    float ThowTypeName<float>::operator() (float f) const {
        return f;
    }
    template <>
    float ThowTypeName<float>::operator() (int i) const {
        return i;
    }

    using ToFloat = ThowTypeName<float>;


    template <>
    int ThowTypeName<int>::operator() (double d) const {
        return d;
    }
    template <>
    int ThowTypeName<int>::operator() (float f) const {
        return f;
    }
    template <>
    int ThowTypeName<int>::operator() (int i) const {
        return i;
    }

    using ToInt = ThowTypeName<int>;


    template <>
    std::string ThowTypeName<std::string>::operator() (const std::string &s) const {
        return s;
    }

    using ToString = ThowTypeName<std::string>;


    template <>
    bool ThowTypeName<bool>::operator() (bool b) const {
        return b;
    }

    using ToBoolean = ThowTypeName<bool>;


}

void BlockDecoder::setFilter(std::unique_ptr<filter::Filter> flt) {
    filter = std::move(flt);

    for(auto &filterPredicate : filter->getPredicates()) {
        const node::Node * filterNode = schemaNodeByPath(filterPredicate->identifier);

        if (filterNode->is<node::Union>()) {
            for( auto &p : filterNode->as<node::Union>().getChildren()) {
                if (filterPredicate->op == filter::equality_expression::IS_NIL ||
                    filterPredicate->op == filter::equality_expression::NOT_NIL
                ) {
                    if (!filterNode->as<node::Union>().containsNull()) {
                        throw std::runtime_error("Field '" + filterPredicate->identifier + "' can not be null");
                    }

                    // TODO lookup for index of NULL-node in the union

                    break;
                }

                filterNode = p.get();

                if (filterNode->is<node::Custom>()) {
                    filterNode = filterNode->as<node::Custom>().getDefinition().get();
                }
                if (filterNode->isOneOf<
                            node::Boolean,
                            node::Double,
                            node::Enum,
                            node::Float,
                            node::Int,
                            node::Long,
                            node::String>()) {
                    break;
                }
            }
        } else {
            if (filterPredicate->op == filter::equality_expression::IS_NIL ||
                filterPredicate->op == filter::equality_expression::NOT_NIL
            ) {
                throw std::runtime_error("Field '" + filterPredicate->identifier + "' can not be null");
            }
        }
        if (filterNode->isOneOf<node::Union>()) {
            ; // ok, acceptable only for isnil/notnil operations
        } else if (filterNode->is<node::Enum>()) {
            auto const &e = filterNode->as<node::Enum>();
            int i = e.findIndexForValue(boost::get<std::string>(filterPredicate->constant));
            if (i == -1) {
                // TODO: add list of valid values
                throw std::runtime_error("Invalid value for enum field '" + filterPredicate->identifier + "'");
            }
            filterPredicate->constant = i;
        } else if (filterNode->is<node::String>()) {
            filterPredicate->constant = convertFilterConstant<ToString>(filterPredicate, filterNode);
        } else if (filterNode->is<node::Boolean>()) {
            filterPredicate->constant = convertFilterConstant<ToBoolean>(filterPredicate, filterNode);
        } else if (filterNode->is<node::Double>()) {
            filterPredicate->constant = convertFilterConstant<ToDouble>(filterPredicate, filterNode);
        } else if (filterNode->is<node::Float>()) {
            filterPredicate->constant = convertFilterConstant<ToFloat>(filterPredicate, filterNode);
        } else if (filterNode->isOneOf<node::Int, node::Long>()) {
            filterPredicate->constant = convertFilterConstant<ToInt>(filterPredicate, filterNode);
        } else {
            throw std::runtime_error(
                "Sorry, but type '" + filterNode->getTypeName() +
                "' for field '" + filterPredicate->identifier + "' "
                "Is not yet supported in filter expression.");
        }
        filterItems.insert(
                std::make_pair(
                    filterNode,
                    std::make_shared<predicate::Predicate>(filterPredicate)
                )
            );
    }

    if (parseLoopEnabled) {
        compileParser(parseLoop, header.schema);
    }

}

template <typename T>
typename T::result_type BlockDecoder::convertFilterConstant(const filter::equality_expression* expr, const node::Node *filterNode) const {
    try {
        return boost::apply_visitor(T(), expr->constant);
    } catch(const TypeName &e) {
        throw std::runtime_error("Invalid type for field '" + expr->identifier + "'"
            + " expected: " + filterNode->getTypeName() + ", got: " +
            e.name);
    }
}

void BlockDecoder::setTsvFilterExpression(const dumper::TsvExpression &tsvFieldsList) {
    this->tsvFieldsList = tsvFieldsList;
}

void BlockDecoder::setDumpMethod(std::function<void(const std::string &)> dumpMethod) {
    this->dumpMethod = dumpMethod;
}

void BlockDecoder::setCountMethod(std::function<void(size_t)> coutMethod) {
    this->coutMethod = coutMethod;
}

void BlockDecoder::enableCountOnlyMode() {
    countOnly = true;
}

void BlockDecoder::enableParseLoop() {
    parseLoopEnabled = true;
}


void BlockDecoder::decodeAndDumpBlock(Block &block) {

    if (countOnly && !filter) {
        // TODO: count without decompression
        coutMethod(block.objectCount);
        return;
    }

    for(int i = 0; i < block.objectCount ; ++i) {
        // TODO: rewrite it using hierarcy of filters/decoders.
        // TODO: implement counter as a filter  
        if (limit.finished()) {
            throw Finished();
        }
        if (block.buffer.eof()) {
            throw Eof();
        }
        block.buffer.startDocument();
        //std::cout << i << " out of << " << block.objectCount << std::endl;
        if (parseLoopEnabled) {
            for(int i = 0; i < parseLoop.size(); ) {
                i += parseLoop[i](block.buffer);
            }
            return;
        } else {
            decodeDocument(block.buffer, header.schema);
        }

        if (!filter || filter->expressionPassed()) {

            dumpDocument(block);

            limit.documentFinished();
            if(filter) {
                filter->resetState();
            }
        }
    }

}

void BlockDecoder::dumpDocument(Block &block) {
    if (countOnly) {
        coutMethod(1);
        return;
    }
    block.buffer.resetToDocument();
    if (tsvFieldsList.pos > 0) {
        dumper::Tsv dumper(tsvFieldsList);
        dumpDocument(block.buffer, header.schema, dumper);
        dumper.EndDocument(dumpMethod);
    } else {
        dumper::Fool dumper;
        dumpDocument(block.buffer, header.schema, dumper);
        dumper.EndDocument(dumpMethod);
    }
}


void BlockDecoder::decodeDocument(DeflatedBuffer &stream, const std::unique_ptr<node::Node> &schema) {
    if (schema->is<node::Record>()) {
        for(auto &p : schema->as<node::Record>().getChildren()) {
            decodeDocument(stream, p);
        }
    } else if (schema->is<node::Union>()) {
        int item = TypeParser<int>::read(stream);
        const auto &node = schema->as<node::Union>().getChildren()[item];
        decodeDocument(stream, node);

        if (filter) {
            auto range = filterItems.equal_range(schema.get());
            if (range.first != range.second) {
                for_each (
                    range.first,
                    range.second,
                    [&node](const auto& filterItem){
                        filterItem.second->setIsNull(node->is<node::Null>());
                    }
                );
            }
        }

    } else if (schema->is<node::Custom>()) {
        decodeDocument(stream, schema->as<node::Custom>().getDefinition());
    } else if (schema->is<node::Enum>()) {
        skipOrApplyFilter<int>(stream, schema);
    } else if (schema->is<node::Array>()) {
        auto const &node = schema->as<node::Array>().getItemsType();

        int objectsInBlock = 0;
        do {
            objectsInBlock = TypeParser<int>::read(stream);
            for(int i = 0; i < objectsInBlock; ++i) {
                decodeDocument(stream, node);
            }
        } while(objectsInBlock != 0);

    } else if (schema->is<node::Map>()) {
        auto const &node = schema->as<node::Map>().getItemsType();

        int objectsInBlock = 0;
        do {
            objectsInBlock = TypeParser<int>::read(stream);

            for(int i = 0; i < objectsInBlock; ++i) {
                TypeParser<StringBuffer>::skip(stream);
                decodeDocument(stream, node);
            }
        } while(objectsInBlock != 0);
    } else {
        if (schema->is<node::String>()) {
            skipOrApplyFilter<StringBuffer>(stream, schema);
        } else if (schema->is<node::Int>()) {
            skipOrApplyFilter<int>(stream, schema);
        } else if (schema->is<node::Long>()) {
            skipOrApplyFilter<long>(stream, schema);
        } else if (schema->is<node::Float>()) {
            skipOrApplyFilter<float>(stream, schema);
        } else if (schema->is<node::Double>()) {
            skipOrApplyFilter<double>(stream, schema);
        } else if (schema->is<node::Boolean>()) {
            skipOrApplyFilter<bool>(stream, schema);
        } else if (schema->is<node::Null>()) {
            ; // empty value: no way to process
        } else {
            std::cout << schema->getItemName() << ":" << schema->getTypeName() << std::endl;
            std::cout << "Can't read type: no decoder. Finishing." << std::endl;
            throw Eof();
        }
    }
}


using parse_func_t = std::function<int(DeflatedBuffer &)>;

std::vector<parse_func_t> parse_items;

class ApplyUnion {
public:
    explicit ApplyUnion(
                    int ret,
                    BlockDecoder::filter_items_t::iterator start,
                    BlockDecoder::filter_items_t::iterator end,
                    int nullIndex)
        : ret(ret),
          start(start),
          end(end),
          nullIndex(nullIndex) {
    }
    int operator() (DeflatedBuffer &stream) {
        const auto &value = TypeParser<int>::read(stream);
        for_each (
            start, end,
            [&value, this](const auto& filterItem){
                filterItem.second->setIsNull(value == this->nullIndex);
            }
        );
        return value + ret;
    }
private:
    int ret;
    BlockDecoder::filter_items_t::iterator start;
    BlockDecoder::filter_items_t::iterator end;
    int nullIndex;
};

class JumpToN {
public:
    JumpToN(int ret, int nullIndex) : ret(ret) {
        (void)nullIndex;
    }
    int operator() (DeflatedBuffer &stream) {
        int i = TypeParser<int>::read(stream);
        return i + ret;
    }
private:
    int ret;
};

class SkipInt {
public:
    explicit SkipInt(int ret):ret(ret) {
    }
    int operator() (DeflatedBuffer &stream) {
        TypeParser<int>::skip(stream);
        return ret;
    }
private:
    int ret;
};

class SkipString {
public:
    explicit SkipString(int ret):ret(ret) {
    }
    int operator() (DeflatedBuffer &stream) {
        TypeParser<StringBuffer>::skip(stream);
        return ret;
    }
private:
    int ret;
};

class ApplyString {
public:
    explicit ApplyString(int ret, BlockDecoder::filter_items_t::iterator start, BlockDecoder::filter_items_t::iterator end)
        : ret(ret),
          start(start),
          end(end) {
    }
    int operator() (DeflatedBuffer &stream) {
        const auto &value = TypeParser<StringBuffer>::read(stream);
        for_each (
            start, end,
            [&value](const auto& filterItem){
                filterItem.second->template apply<StringBuffer>(value);
            }
        );
        return ret;
    }
private:
    int ret;
    BlockDecoder::filter_items_t::iterator start;
    BlockDecoder::filter_items_t::iterator end;
};

template <typename T>
class Apply {
public:
    explicit Apply(int ret, BlockDecoder::filter_items_t::iterator start, BlockDecoder::filter_items_t::iterator end)
        : ret(ret),
          start(start),
          end(end) {
    }
    int operator() (DeflatedBuffer &stream) {
        const auto &value = TypeParser<T>::read(stream);
        for_each (
            start, end,
            [&value](const auto& filterItem){
                filterItem.second->template apply<T>(value);
            }
        );
        return ret;
    }
private:
    int ret;
    BlockDecoder::filter_items_t::iterator start;
    BlockDecoder::filter_items_t::iterator end;
};


template <int n>
class SkipNBytes {
public:
    explicit SkipNBytes(int ret):ret(ret) {
    }
    int operator() (DeflatedBuffer &stream) {
        stream.skip(n);
        return ret;
    }
private:
    int ret;
};


class SkipArray {
public:
    explicit SkipArray(int ret, BlockDecoder::const_node_t &schema, BlockDecoder &decoder)
        : ret(ret),
          nodeType(schema),
          decoder(decoder) {
    }
    int operator() (DeflatedBuffer &stream) {

        int objectsInBlock = 0;
        do {
            objectsInBlock = TypeParser<int>::read(stream);
            for(int i = 0; i < objectsInBlock; ++i) {
                decoder.decodeDocument(stream, nodeType);
            }
        } while(objectsInBlock != 0);
        return ret;
    }
private:
    int ret;
    BlockDecoder::const_node_t &nodeType;
    BlockDecoder &decoder;
};

class SkipMap {
public:
    explicit SkipMap(int ret, BlockDecoder::const_node_t &schema, BlockDecoder &decoder)
        : ret(ret),
          nodeType(schema),
          decoder(decoder) {
    }
    int operator() (DeflatedBuffer &stream) {

        int objectsInBlock = 0;
        do {
            objectsInBlock = TypeParser<int>::read(stream);

            for(int i = 0; i < objectsInBlock; ++i) {
                TypeParser<StringBuffer>::skip(stream);
                decoder.decodeDocument(stream, nodeType);
            }
        } while(objectsInBlock != 0);
        return ret;
    }
private:
    int ret;
    BlockDecoder::const_node_t &nodeType;
    BlockDecoder &decoder;
};


int BlockDecoder::compileParser(std::vector<parse_func_t> &parse_items, const std::unique_ptr<node::Node> &schema, int elementsToSkip) {

    if (schema->is<node::Union>()) {

        const auto &u = schema->as<node::Union>();

        size_t nullIndex = -1; // u.nullIndex();

        auto size = u.getChildren().size();

        auto &children = u.getChildren();
        int i = size;

        int elementsLeft = elementsToSkip;
        for(auto c = children.rbegin(); c != children.rend(); ++c) {
            --i;
            auto &p = *c;
            if (p->is<node::Null>()) {
                nullIndex = i;
            }
            elementsLeft += compileParser(parse_items, p, elementsLeft);
        }

        skipOrApplyCompileFilter_r<JumpToN, ApplyUnion>(parse_items, schema, elementsToSkip, nullIndex);

        return elementsLeft;
    } else if (schema->is<node::Custom>()) {
        return compileParser(parse_items, schema->as<node::Custom>().getDefinition());
    } else if (schema->is<node::Record>()) {
        auto &children = schema->as<node::Record>().getChildren();
        size_t size = 0;
        for(auto c = children.rbegin(); c != children.rend(); ++c) {
            size += compileParser(parse_items, *c);
        }
        return size;
    } else if (schema->is<node::Array>()) {
            auto it = parse_items.begin();
            parse_items.emplace(it, SkipArray(elementsToSkip, schema->as<node::Array>().getItemsType(), *this));
            return 1;
    } else if (schema->is<node::Map>()) {
            auto it = parse_items.begin();
            parse_items.emplace(it, SkipMap(elementsToSkip, schema->as<node::Map>().getItemsType(), *this));
            return 1;
    } else {
        if (schema->is<node::String>()) {
            skipOrApplyCompileFilter_r<SkipString, ApplyString>(parse_items, schema, elementsToSkip);
        } else if (schema->isOneOf<node::Enum, node::Int, node::Long>()) {
            skipOrApplyCompileFilter_r<SkipInt, Apply<int>>(parse_items, schema, elementsToSkip);
        } else if (schema->is<node::Float>()) {
            skipOrApplyCompileFilter_r<SkipNBytes<4>, Apply<float>>(parse_items, schema, elementsToSkip);
        } else if (schema->is<node::Double>()) {
            skipOrApplyCompileFilter_r<SkipNBytes<8>, Apply<double>>(parse_items, schema, elementsToSkip);
        } else if (schema->is<node::Boolean>()) {
            skipOrApplyCompileFilter_r<SkipNBytes<1>, Apply<bool>>(parse_items, schema, elementsToSkip);
        } else if (schema->is<node::Null>()) {
            auto it = parse_items.begin();
            parse_items.emplace(it, SkipNBytes<0>(elementsToSkip));
            return 1; // Empty item. TODO: do not even insert it
        } else {
            std::cout << schema->getItemName() << ":" << schema->getTypeName() << std::endl;
            std::cout << "compileParser: Can't read type: no decoder. Finishing." << std::endl;
            throw Eof();
        }

        return 1;
    }
}

template <typename SkipType, typename ApplyType, typename... Args>
void BlockDecoder::skipOrApplyCompileFilter_r(std::vector<parse_func_t> &parse_items, const std::unique_ptr<node::Node> &schema, int ret, Args... args) {
    auto it = parse_items.begin();
    if (filter) {
        auto range = filterItems.equal_range(schema.get());
        if (range.first != range.second) {
            parse_items.emplace(it, ApplyType(ret, range.first, range.second, args...));
            return;
        }
    }
    parse_items.emplace(it, SkipType(ret, args...));
}

template <typename SkipType, typename ApplyType, typename... Args>
void BlockDecoder::skipOrApplyCompileFilter(std::vector<parse_func_t> &parse_items, const std::unique_ptr<node::Node> &schema, int ret, Args... args) {
    if (filter) {
        auto range = filterItems.equal_range(schema.get());
        if (range.first != range.second) {
            parse_items.emplace_back(ApplyType(ret, range.first, range.second, args...));
            return;
        }
    }
    parse_items.emplace_back(SkipType(ret, args...));
}

template <class T>
void BlockDecoder::dumpDocument(DeflatedBuffer &stream, const std::unique_ptr<node::Node> &schema, T &dumper) {
    if (schema->is<node::Record>()) {

        auto &r = schema->as<node::Record>();
        dumper.RecordBegin(r);
        for(auto &p : schema->as<node::Record>().getChildren()) {
            dumpDocument(stream, p, dumper);
        }
        dumper.RecordEnd(r);

    } else if (schema->is<node::Union>()) {
        int item = readZigZagLong(stream);
        const auto &node = schema->as<node::Union>().getChildren()[item];
        dumpDocument(stream, node, dumper);
    } else if (schema->is<node::Custom>()) {
        dumper.CustomBegin(schema->as<node::Custom>());
        dumpDocument(stream, schema->as<node::Custom>().getDefinition(), dumper);
    } else if (schema->is<node::Enum>()) {
        int index = readZigZagLong(stream);
        dumper.Enum(schema->as<node::Enum>(), index);

    } else if (schema->is<node::Array>()) {
        auto &a = schema->as<node::Array>();
        auto const &node = a.getItemsType();

        dumper.ArrayBegin(a);
        int objectsInBlock = 0;
        do {
            objectsInBlock = readZigZagLong(stream);
            for(int i = 0; i < objectsInBlock; ++i) {
                dumpDocument(stream, node, dumper);
            }
        } while(objectsInBlock != 0);
        dumper.ArrayEnd(a);

    } else if (schema->is<node::Map>()) {
        auto &m = schema->as<node::Map>();
        auto const &node = m.getItemsType();

        // TODO: refactor this trash
        assert(node->is<node::String>() || node->is<node::Int>());
        dumper.MapBegin(m);
        int objectsInBlock = 0;
        do {
            objectsInBlock = readZigZagLong(stream);

            for(int i = 0; i < objectsInBlock; ++i) {
                const auto & name  = stream.getString(readZigZagLong(stream));
                dumper.MapName(name);

                // TODO: refactor this trash
                if (node->is<node::String>()) {
                    const auto & value = stream.getString(readZigZagLong(stream));
                    dumper.MapValue(value, node->as<node::String>());
                } else if (node->is<node::Int>()) {
                     const auto & value = readZigZagLong(stream);
                    dumper.MapValue(value, node->as<node::Int>());
               }
            }
        } while(objectsInBlock != 0);
        dumper.MapEnd(m);
    } else {
        if (schema->is<node::String>()) {
            dumper.String(stream.getString(readZigZagLong(stream)), schema->as<node::String>());
        } else if (schema->is<node::Int>()) {
            int value = readZigZagLong(stream);
            dumper.Int(value, schema->as<node::Int>());
            // std::cout << schema->getItemName() << ": " << value << std::endl;
        } else if (schema->is<node::Long>()) {
            long value = readZigZagLong(stream);
            dumper.Long(value, schema->as<node::Long>());
        } else if (schema->is<node::Float>()) {
            float value = TypeParser<float>::read(stream);
            dumper.Float(value, schema->as<node::Float>());
        } else if (schema->is<node::Double>()) {
            double value =  TypeParser<double>::read(stream);
            dumper.Double(value, schema->as<node::Double>());
        } else if (schema->is<node::Boolean>()) {
            bool value = TypeParser<bool>::read(stream);
            dumper.Boolean(value, schema->as<node::Boolean>());
        } else if (schema->is<node::Null>()) {
            dumper.Null(schema->as<node::Null>());
        } else {
            std::cout << schema->getItemName() << ":" << schema->getTypeName() << std::endl;
            std::cout << "Can't read type: no decoder. Finishing." << std::endl;
            throw Eof();
        }
    }
}


template <typename T>
void BlockDecoder::skipOrApplyFilter(DeflatedBuffer &stream, const std::unique_ptr<node::Node> &schema) {
    if (filter) {
        auto range = filterItems.equal_range(schema.get());
        if (range.first != range.second) {
            const auto &value = TypeParser<T>::read(stream);
            for_each (
                range.first,
                range.second,
                [&value](const auto& filterItem){
                    filterItem.second->template apply<T>(value);
                }
            );
            return;
        }
    }
    TypeParser<T>::skip(stream);
}

const node::Node* BlockDecoder::schemaNodeByPath(const std::string &path) {
    auto n = node::nodeByPath(path, header.schema.get());
    if (n == nullptr) {
        throw PathNotFound(path);
    }
    return n;
}


}
