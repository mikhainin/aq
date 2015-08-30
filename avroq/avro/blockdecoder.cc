
#include <filter/filter.h>
#include <filter/equality_expression.h>
#include <filter/record_expression.h>

#include "dumper/fool.h"
#include "dumper/json.h"
#include "dumper/tsv.h"

#include "node/all_nodes.h"

#include "predicate/list.h"
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


void BlockDecoder::setFilter(std::unique_ptr<filter::Filter> flt) {

    predicates.reset(new predicate::List(std::move(flt), header.schema.get()));

    if (parseLoopEnabled) {
        compileFilteringParser(parseLoop, header.schema);
    }

}

void BlockDecoder::setTsvFilterExpression(const dumper::TsvExpression &tsvFieldsList) {
    this->tsvFieldsList = tsvFieldsList;

    if (parseLoopEnabled) {
        compileTsvExpression(tsvDumpLoop, header.schema);
    }
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

void BlockDecoder::outputAsJson(bool pretty) {
    jsonMode = true;
    if (pretty) {
        jsonPrettyMode = true;
    }
}


void BlockDecoder::decodeAndDumpBlock(Block &block) {

    if (countOnly && !predicates) {
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
            for(size_t i = 0; i < parseLoop.size(); ) {
                i += parseLoop[i](block.buffer);
            }
        } else {
            decodeDocument(block.buffer, header.schema);
        }

        if (!predicates || predicates->expressionPassed()) {

            limit.documentFinished();

            dumpDocument(block);

        }
        if(predicates) {
            predicates->resetState();
        }
    }

}

void BlockDecoder::dumpDocument(Block &block) {
    if (countOnly) {
        coutMethod(1);
        return;
    }
    /*void _dump_deb();
    _dump_deb();
    std::cout << "===================\n";*/
    block.buffer.resetToDocument();
    if (tsvFieldsList.pos > 0) {
        dumper::Tsv dumper(tsvFieldsList);
        if (parseLoopEnabled) {
            for(size_t i = 0; i < tsvDumpLoop.size(); ) {
                // std::cout << "i=" << i << std::endl;
                i += tsvDumpLoop[i](block.buffer, dumper);
            }
        } else {
            dumpDocument(block.buffer, header.schema, dumper);
        }
        dumper.EndDocument(dumpMethod);
    } else if (jsonMode) {
        if (jsonPrettyMode) {
            dumper::Json<dumper::JsonTag::pretty> dumper;
            dumpDocument(block.buffer, header.schema, dumper);
            dumper.EndDocument(dumpMethod);
        } else {
            dumper::Json<dumper::JsonTag::plain> dumper;
            dumpDocument(block.buffer, header.schema, dumper);
            dumper.EndDocument(dumpMethod);
        }
    } else {
        dumper::Fool dumper;
        dumpDocument(block.buffer, header.schema, dumper);
        dumper.EndDocument(dumpMethod);
    }

    // std::cout.flush();
}


void BlockDecoder::decodeDocument(DeflatedBuffer &stream, const std::unique_ptr<node::Node> &schema) {
    if (schema->is<node::Record>()) {
        for(auto &p : schema->as<node::Record>().getChildren()) {
            decodeDocument(stream, p);
        }
        if (predicates) {
            auto range = predicates->getEqualRange(schema.get());
            if (range.first != range.second) {
                for_each (
                    range.first,
                    range.second,
                    [](const auto& filterItem){
                        filterItem.second->recordEnd();
                    }
                );
            }
        }
    } else if (schema->is<node::Union>()) {
        int item = TypeParser<int>::read(stream);
        const auto &node = schema->as<node::Union>().getChildren()[item];
        decodeDocument(stream, node);

        if (predicates) {
            auto range = predicates->getEqualRange(schema.get());
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

            decltype(predicates->getEqualRange(schema.get())) range;
            if (predicates) {
                range = predicates->getEqualRange(schema.get());
            }
            for(int i = 0; i < objectsInBlock; ++i) {
                decodeDocument(stream, node);
                if (range.first != range.second) {
                    for_each (
                        range.first, range.second,
                        [](const auto& filterItem){
                            filterItem.second->pushArrayState();
                        }
                    );
                }
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
            std::cerr << schema->getItemName() << ":" << schema->getTypeName() << std::endl;
            std::cerr << "Can't read type: no decoder. Finishing." << std::endl;
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
                    predicate::List::filter_items_t::iterator start,
                    predicate::List::filter_items_t::iterator end,
                    int nullIndex,
                    std::vector<int> &skipping)
        : ret(ret),
          start(start),
          end(end),
          nullIndex(nullIndex),
          skipping(skipping) {
    }
    int operator() (DeflatedBuffer &stream) {
        const auto &value = TypeParser<int>::read(stream);
        for_each (
            start, end,
            [&value, this](const auto& filterItem){
                filterItem.second->setIsNull(value == this->nullIndex);
            }
        );
        return skipping[value];
    }
private:
    int ret;
    predicate::List::filter_items_t::iterator start;
    predicate::List::filter_items_t::iterator end;
    int nullIndex;
    std::vector<int> skipping;
};

class JumpToN {
public:
    JumpToN(int ret, int nullIndex, std::vector<int> &skipping)
        : skipping(skipping) {
        (void)nullIndex;
    }
    int operator() (DeflatedBuffer &stream) {
        int i = TypeParser<int>::read(stream);
        return skipping[i];
    }
private:
    std::vector<int> skipping;
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
    explicit ApplyString(int ret, predicate::List::filter_items_t::iterator start, predicate::List::filter_items_t::iterator end)
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
    predicate::List::filter_items_t::iterator start;
    predicate::List::filter_items_t::iterator end;
};

template <typename T>
class Apply {
public:
    explicit Apply(int ret, predicate::List::filter_items_t::iterator start, predicate::List::filter_items_t::iterator end)
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
    predicate::List::filter_items_t::iterator start;
    predicate::List::filter_items_t::iterator end;
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
    int operator() (DeflatedBuffer &stream, dumper::Tsv &tsv) {
        (void)tsv;
        return operator() (stream);
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
    int operator() (DeflatedBuffer &stream, dumper::Tsv &tsv) {
        (void)tsv;
        return operator() (stream);
    }
private:
    int ret;
    BlockDecoder::const_node_t &nodeType;
    BlockDecoder &decoder;
};


class ApplyArray {
public:

    explicit ApplyArray(int ret,
        predicate::List::filter_items_t::iterator start,
        predicate::List::filter_items_t::iterator end,
        BlockDecoder::const_node_t &schema,
        BlockDecoder &decoder)
        : ret(ret),
          nodeType(schema),
          decoder(decoder),
          start(start),
          end(end) {
    }
    int operator() (DeflatedBuffer &stream) {

        int objectsInBlock = 0;
        do {
            objectsInBlock = TypeParser<int>::read(stream);
            for(int i = 0; i < objectsInBlock; ++i) {
                decoder.decodeDocument(stream, nodeType);
                for_each (
                    start, end,
                    [](const auto& filterItem){
                        filterItem.second->pushArrayState();
                    }
                );
            }
        } while(objectsInBlock != 0);
        return ret;
    }
    int operator() (DeflatedBuffer &stream, dumper::Tsv &tsv) {
        (void)tsv;
        return operator() (stream);
    }
private:
    int ret;
    BlockDecoder::const_node_t &nodeType;
    BlockDecoder &decoder;
    predicate::List::filter_items_t::iterator start;
    predicate::List::filter_items_t::iterator end;
};



class ApplyRecord {
public:

    explicit ApplyRecord(int ret,
        predicate::List::filter_items_t::iterator start,
        predicate::List::filter_items_t::iterator end)
        : ret(ret),
          start(start),
          end(end) {
    }
    int operator() (DeflatedBuffer &stream) {

        for_each (
            start, end,
            [](const auto& filterItem){
                filterItem.second->recordEnd();
            }
        );
        return ret;
    }
    int operator() (DeflatedBuffer &stream, dumper::Tsv &tsv) {
        (void)tsv;
        return operator() (stream);
    }
private:
    int ret;
    predicate::List::filter_items_t::iterator start;
    predicate::List::filter_items_t::iterator end;
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
    int operator() (DeflatedBuffer &stream, dumper::Tsv &tsv) {
        (void)tsv;
        return operator() (stream);
    }
private:
    int ret;
    BlockDecoder::const_node_t &nodeType;
    BlockDecoder &decoder;
};


int BlockDecoder::compileFilteringParser(std::vector<parse_func_t> &parse_items, const std::unique_ptr<node::Node> &schema, int elementsToSkip) {

    if (schema->is<node::Union>()) {

        const auto &u = schema->as<node::Union>();

        size_t nullIndex = -1;

        auto size = u.getChildren().size();

        auto &children = u.getChildren();
        int i = size;

        std::vector<int> skipping;
        int elementsLeft = elementsToSkip;
        for(auto c = children.rbegin(); c != children.rend(); ++c) {
            --i;
            auto &p = *c;
            if (p->is<node::Null>()) {
                nullIndex = i;
            }
            elementsLeft += compileFilteringParser(parse_items, p, elementsLeft);
            skipping.push_back(elementsLeft - elementsToSkip);
        }

        skipOrApplyCompileFilter<JumpToN, ApplyUnion>(parse_items, schema, elementsToSkip, nullIndex, skipping);

        return elementsLeft;
    } else if (schema->is<node::Custom>()) {
        return compileFilteringParser(parse_items, schema->as<node::Custom>().getDefinition());
    } else if (schema->is<node::Record>()) {
            if (predicates) {
                auto range = predicates->getEqualRange(schema.get());
                if (range.first != range.second) {
                    auto it = parse_items.begin();
                    parse_items.emplace(it, ApplyRecord(elementsToSkip, range.first, range.second));
                }
            }
        auto &children = schema->as<node::Record>().getChildren();
        size_t size = 1;
        for(auto c = children.rbegin(); c != children.rend(); ++c) {
            size += compileFilteringParser(parse_items, *c);
        }
        return size;
    } else if (schema->is<node::Array>()) {

            auto it = parse_items.begin();
            if (predicates) {
                auto range = predicates->getEqualRange(schema.get());
                if (range.first != range.second) {
                    parse_items.emplace(it, ApplyArray(elementsToSkip, range.first, range.second, schema->as<node::Array>().getItemsType(), *this));
                    return 1;
                }
            }
            parse_items.emplace(it, SkipArray(elementsToSkip, schema->as<node::Array>().getItemsType(), *this));
            return 1;
    } else if (schema->is<node::Map>()) {
            auto it = parse_items.begin();
            parse_items.emplace(it, SkipMap(elementsToSkip, schema->as<node::Map>().getItemsType(), *this));
            return 1;
    } else {
        if (schema->is<node::String>()) {
            skipOrApplyCompileFilter<SkipString, ApplyString>(parse_items, schema, elementsToSkip);
        } else if (schema->isOneOf<node::Enum, node::Int, node::Long>()) {
            skipOrApplyCompileFilter<SkipInt, Apply<int>>(parse_items, schema, elementsToSkip);
        } else if (schema->is<node::Float>()) {
            skipOrApplyCompileFilter<SkipNBytes<4>, Apply<float>>(parse_items, schema, elementsToSkip);
        } else if (schema->is<node::Double>()) {
            skipOrApplyCompileFilter<SkipNBytes<8>, Apply<double>>(parse_items, schema, elementsToSkip);
        } else if (schema->is<node::Boolean>()) {
            skipOrApplyCompileFilter<SkipNBytes<1>, Apply<bool>>(parse_items, schema, elementsToSkip);
        } else if (schema->is<node::Null>()) {
            auto it = parse_items.begin();
            parse_items.emplace(it, SkipNBytes<0>(elementsToSkip));
            return 1; // Empty item. TODO: do not even insert it
        } else {
            // TODO fire runtime_error here
            std::cout << schema->getItemName() << ":" << schema->getTypeName() << std::endl;
            std::cout << "compileParser: Can't read type: no decoder. Finishing." << std::endl;
            throw Eof();
        }

        return 1;
    }
}

template <typename SkipType, typename ApplyType, typename... Args>
void BlockDecoder::skipOrApplyCompileFilter(std::vector<parse_func_t> &parse_items, const std::unique_ptr<node::Node> &schema, int ret, Args... args) {
    auto it = parse_items.begin();
    if (predicates) {
        auto range =  predicates->getEqualRange(schema.get());
        if (range.first != range.second) {
            parse_items.emplace(it, ApplyType(ret, range.first, range.second, args...));
            return;
        }
    }
    parse_items.emplace(it, SkipType(ret, args...));
}


template<typename T>
class ApplyTsv {
public:
    ApplyTsv(int ret,
             dumper::TsvExpression::map_t::iterator start,
             dumper::TsvExpression::map_t::iterator end)
        : ret(ret),
          start(start),
          end(end) {
    }
    int operator() (DeflatedBuffer &stream, dumper::Tsv &tsv) {
        const auto &value = TypeParser<T>::read(stream);
        for_each (
            start, end,
            [&value, &tsv, this](const auto& tsvItem){
                tsv.addToPosition(value, tsvItem.second);
            }
        );
        return ret;
    }
private:
    int ret;
    dumper::TsvExpression::map_t::iterator start;
    dumper::TsvExpression::map_t::iterator end;
};


class ApplyTsvNull {
public:
    ApplyTsvNull(int ret,
                 dumper::TsvExpression::map_t::iterator start,
                 dumper::TsvExpression::map_t::iterator end)
        : ret(ret),
          start(start),
          end(end) {
    }
    int operator() (DeflatedBuffer &stream, dumper::Tsv &tsv) {
        for_each (
            start, end,
            [&tsv, this](const auto& tsvItem){
                tsv.addToPosition(stringifyedNull, tsvItem.second);
            }
        );
        return ret;
    }
private:
    int ret;
    const std::string stringifyedNull = "null";
    dumper::TsvExpression::map_t::iterator start;
    dumper::TsvExpression::map_t::iterator end;
};

class ApplyTsvEnum {
public:
    ApplyTsvEnum(int ret,
                 dumper::TsvExpression::map_t::iterator start,
                 dumper::TsvExpression::map_t::iterator end,
                 const std::vector<std::string> &items)
        : ret(ret),
          start(start),
          end(end),
          items(items) {
    }
    int operator() (DeflatedBuffer &stream, dumper::Tsv &tsv) {
        int item = TypeParser<int>::read(stream);

        for_each (
            start, end,
            [&item, &tsv, this](const auto& tsvItem){
                tsv.addToPosition(items[item], tsvItem.second);
            }
        );

        return ret;
    }
private:
    const std::string stringifyedNull = "null";
    int ret;
    dumper::TsvExpression::map_t::iterator start;
    dumper::TsvExpression::map_t::iterator end;
    const std::vector<std::string> items;
};

class SkipTsvEnum {
public:
    SkipTsvEnum(int ret, const std::vector<std::string> &items)
        : ret(ret) {
          (void)items;
    }
    int operator() (DeflatedBuffer &stream, dumper::Tsv &tsv) {
        TypeParser<int>::skip(stream);
        return ret;
    }
private:
    int ret;
};

class ApplyTsvAsJson {
public:
    ApplyTsvAsJson(int ret,
        dumper::TsvExpression::map_t::iterator start,
        dumper::TsvExpression::map_t::iterator end,
        BlockDecoder::const_node_t &schema,
        BlockDecoder &decoder)
        : ret(ret),
          nodeType(schema),
          decoder(decoder),
          start(start),
          end(end) {
    }
    int operator() (DeflatedBuffer &stream, dumper::Tsv &tsv) {
        dumper::Json<dumper::JsonTag::plain> dumper;

        decoder.dumpDocument(stream, nodeType, dumper);

        const auto &value = dumper.GetAsString();

        for_each (
            start, end,
            [&value, &tsv, this](const auto& tsvItem){
                tsv.addToPosition(value, tsvItem.second);
            }
        );

        return ret;

    }

private:
    int ret;
    BlockDecoder::const_node_t &nodeType;
    BlockDecoder &decoder;
    dumper::TsvExpression::map_t::iterator start;
    dumper::TsvExpression::map_t::iterator end;
};

template <typename T>
class ParseToTsvAdapter : public T {
public:
    template<typename... Args>
    ParseToTsvAdapter(Args... args)
        : T(args...) {
    }

    int operator() (DeflatedBuffer &stream, dumper::Tsv &tsv) {
        return this->T::operator ()(stream);
    }

};

class JumpToNTsv {
public:
    JumpToNTsv(std::vector<int> &skipping)
        : skipping(skipping) {
    }

    int operator() (DeflatedBuffer &stream, dumper::Tsv &tsv) {
        (void)tsv;
        int i = TypeParser<int>::read(stream);
        return skipping[i];
    }

private:
    std::vector<int> skipping;
};
/*
#include <list>
#include <tuple>

std::list<std::tuple<int,std::string, std::vector<int>>> debugTsvExpression;
void _deb(int i, const std::unique_ptr<node::Node> &schema) {
    debugTsvExpression.push_front(
        std::tuple<int,std::string, std::vector<int>>(
            i, schema->getItemName() + ':' + schema->getTypeName(), std::vector<int>())
    );
}

void _deb(int i, const std::unique_ptr<node::Node> &schema, std::vector<int> &v) {
    debugTsvExpression.push_front(
        std::tuple<int,std::string, std::vector<int>>(
            i, schema->getItemName() + ':' + schema->getTypeName(), v)
    );
}*/
/*
void _dump_deb() {
    int num = 0;
    for(auto &i : debugTsvExpression) {
        std::cout << num++ << ' ' << std::get<0>(i) << ' ' << std::get<1>(i);

        if (std::get<2>(i).size() > 0) {
            std::cout << '[';
            for (int x : std::get<2>(i)) {
                std::cout << x << ',';
            }
            std::cout << ']';
        }

        std::cout << std::endl;
    }
}
*/
int BlockDecoder::compileTsvExpression(std::vector<dump_tsv_func_t> &parse_items, const std::unique_ptr<node::Node> &schema, int elementsToSkip) {

    if (schema->is<node::Union>()) {

        const auto &u = schema->as<node::Union>();

        auto &children = u.getChildren();

        std::vector<int> skipping;
        int elementsLeft = elementsToSkip;
        for(auto c = children.rbegin(); c != children.rend(); ++c) {
            // auto p = skipping.begin();
            elementsLeft += compileTsvExpression(parse_items, *c, elementsLeft);
            skipping.push_back(elementsLeft - elementsToSkip);
        }
        /*if (schema->getItemName() == "demand") {
            std::cout << "======= " << elementsToSkip << ' ' << elementsLeft << std::endl;
        }*/
        //_deb(elementsToSkip, schema, skipping);
        auto it = parse_items.begin();
        parse_items.emplace(it, JumpToNTsv(skipping));

        return elementsLeft;
    } else if (schema->is<node::Custom>()) {
        return compileTsvExpression(parse_items, schema->as<node::Custom>().getDefinition());
    } else if (schema->is<node::Record>()) {
        auto p = tsvFieldsList.what.equal_range(schema->getNumber());
        if (p.first != p.second) {
            auto it = parse_items.begin();
            parse_items.emplace(it, ApplyTsvAsJson(elementsToSkip, p.first, p.second, schema, *this));
            return 1;
        } else {
            auto &children = schema->as<node::Record>().getChildren();
            size_t size = 0;
            for(auto c = children.rbegin(); c != children.rend(); ++c) {
                size += compileTsvExpression(parse_items, *c);
            }
            return size;
        }

    } else if (schema->is<node::Array>()) {
        //_deb(elementsToSkip, schema);
        auto p = tsvFieldsList.what.equal_range(schema->getNumber());
        auto it = parse_items.begin();
        if (p.first != p.second) {
            parse_items.emplace(it, ApplyTsvAsJson(elementsToSkip, p.first, p.second, schema, *this));
        } else {
            parse_items.emplace(it, SkipArray(elementsToSkip, schema->as<node::Array>().getItemsType(), *this));
        }
        return 1;
    } else if (schema->is<node::Map>()) {
        //_deb(elementsToSkip, schema);
        auto p = tsvFieldsList.what.equal_range(schema->getNumber());
        auto it = parse_items.begin();
        if (p.first != p.second) {
            parse_items.emplace(it, ApplyTsvAsJson(elementsToSkip, p.first, p.second, schema, *this));
        } else {
            parse_items.emplace(it, SkipMap(elementsToSkip, schema->as<node::Map>().getItemsType(), *this));
        }
        return 1;
    } else {
        if (schema->is<node::String>()) {
            //_deb(elementsToSkip, schema);
            skipOrApplyTsvExpression<ParseToTsvAdapter<SkipString>, ApplyTsv<StringBuffer>>(parse_items, schema, elementsToSkip);
        } else if (schema->isOneOf<node::Int, node::Long>()) {
            //_deb(elementsToSkip, schema);
            skipOrApplyTsvExpression<ParseToTsvAdapter<SkipInt>, ApplyTsv<int>>(parse_items, schema, elementsToSkip);
        } else if (schema->is<node::Enum>()) {
            //_deb(elementsToSkip, schema);
            skipOrApplyTsvExpression<SkipTsvEnum, ApplyTsvEnum>(
                    parse_items,
                    schema,
                    elementsToSkip,
                    schema->as<node::Enum>().getItems()
                );
        } else if (schema->is<node::Float>()) {
            //_deb(elementsToSkip, schema);
            skipOrApplyTsvExpression<SkipNBytes<4>, ApplyTsv<float>>(parse_items, schema, elementsToSkip);
        } else if (schema->is<node::Double>()) {
            //_deb(elementsToSkip, schema);
            skipOrApplyTsvExpression<SkipNBytes<8>, ApplyTsv<double>>(parse_items, schema, elementsToSkip);
        } else if (schema->is<node::Boolean>()) {
            //_deb(elementsToSkip, schema);
            skipOrApplyTsvExpression<SkipNBytes<1>, ApplyTsv<bool>>(parse_items, schema, elementsToSkip);
        } else if (schema->is<node::Null>()) {
            //_deb(elementsToSkip, schema);
            skipOrApplyTsvExpression<SkipNBytes<0>, ApplyTsvNull>(parse_items, schema, elementsToSkip);
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
void BlockDecoder::skipOrApplyTsvExpression(std::vector<dump_tsv_func_t> &parse_items, const std::unique_ptr<node::Node> &schema, int ret, Args... args) {
    auto it = parse_items.begin();
    auto p = tsvFieldsList.what.equal_range(schema->getNumber());
    if (p.first != p.second) {
        parse_items.emplace(it, ApplyType(ret, p.first, p.second, args...));
        return;
    }
    parse_items.emplace(it, SkipType(ret, args...));
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
    if (predicates) {
        auto range = predicates->getEqualRange(schema.get());
        if (range.first != range.second) {
            const auto &value = TypeParser<T>::read(stream);
            for_each (
                range.first,
                range.second,
                [&value](const auto& filterItem) {
                    filterItem.second->template apply<T>(value);
                }
            );
            return;
        }
    }
    TypeParser<T>::skip(stream);
}


}
