
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

    struct ToDouble {
        using result_type = double;
        double operator() (double d) const {
            return d;
        }
        double operator() (float f) const {
            return f;
        }
        double operator() (int i) const {
            return i;
        }

        template <typename T>
        double operator() (const T&) const {
            throw std::runtime_error("wrong type");
        }
    };


    struct ToFloat {
        using result_type = float;
        float operator() (double d) const {
            return d;
        }
        float operator() (float f) const {
            return f;
        }
        float operator() (int i) const {
            return i;
        }

        template <typename T>
        float operator() (const T&) const {
            throw std::runtime_error("wrong type");
        }
    };

    struct ToInt {
        using result_type = int;
        int operator() (double d) const {
            return d;
        }
        int operator() (float f) const {
            return f;
        }
        int operator() (int i) const {
            return i;
        }

        template <typename T>
        float operator() (const T&) const {
            throw std::runtime_error("wrong type");
        }
    };

}

void BlockDecoder::setTsvFilterExpression(const dumper::TsvExpression &tsvFieldsList) {
    this->tsvFieldsList = tsvFieldsList;
}

void BlockDecoder::setDumpMethod(std::function<void(const std::string &)> dumpMethod) {
    this->dumpMethod = dumpMethod;
}

void BlockDecoder::setFilter(std::unique_ptr<filter::Filter> flt) {
    filter = std::move(flt);

    for(auto &filterPredicate : filter->getPredicates()) {
        const node::Node * filterNode = schemaNodeByPath(filterPredicate->identifier);

        if (filterNode->is<node::Union>()) {
            for( auto &p : filterNode->as<node::Union>().getChildren()) {
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
        }
        if (filterNode->isOneOf<node::String, node::Boolean>()) {
            ; // ok
        } else if (filterNode->is<node::Enum>()) {
            auto const &e = filterNode->as<node::Enum>();
            int i = e.findIndexForValue(boost::get<std::string>(filterPredicate->constant));
            if (i == -1) {
                // TODO: add list of valid values
                throw std::runtime_error("Invalid value for enum field '" + filterPredicate->identifier + "'");
            }
            filterPredicate->constant = i;
        } else if (filterNode->is<node::Double>()) {
            filterPredicate->constant = boost::apply_visitor(ToDouble(), filterPredicate->constant);
        } else if (filterNode->is<node::Float>()) {
            filterPredicate->constant = boost::apply_visitor(ToFloat(), filterPredicate->constant);
        } else if (filterNode->isOneOf<node::Int, node::Long>()) {
            filterPredicate->constant = boost::apply_visitor(ToInt(), filterPredicate->constant);
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
}


void BlockDecoder::decodeAndDumpBlock(Block &block) {

    for(int i = 0; i < block.objectCount; ++i) {
        // TODO: rewrite it using hierarcy of filters/decoders.
        // TODO: implement counter as a filter  
        if (limit.finished() || block.buffer.eof()) {
            throw Finished();
        }
        block.buffer.startDocument();
        decodeDocument(block.buffer, header.schema);

        if (!filter || filter->expressionPassed()) {
            block.buffer.resetToDocument();
            if (tsvFieldsList.pos > 0) {
                auto dumper = dumper::Tsv(tsvFieldsList);
                dumpDocument(block.buffer, header.schema, dumper);
                dumper.EndDocument(dumpMethod);
            } else {
                auto dumper = dumper::Fool();
                dumpDocument(block.buffer, header.schema, dumper);
                dumper.EndDocument(dumpMethod);
            }
            limit.documentFinished();
            if(filter) {
                filter->resetState();
            }
        }
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
            objectsInBlock = readZigZagLong(stream);

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
            // std::cout << schema->getItemName() << ": " << value << std::endl;
        } else if (schema->is<node::Float>()) {
            // readFloat(stream);
            // std::cout << schema->getItemName() << ": " << readFloat(stream) << std::endl;
            float value = TypeParser<float>::read(stream);
            dumper.Float(value, schema->as<node::Float>());
        } else if (schema->is<node::Double>()) {
            // std::cout << schema->getItemName() << ": " << readDouble(stream) << std::endl;
            double value =  TypeParser<double>::read(stream);
            dumper.Double(value, schema->as<node::Double>());
        } else if (schema->is<node::Boolean>()) {
            // std::cout << schema->getItemName() << ": " << readBoolean(stream) << std::endl;
            bool value = TypeParser<bool>::read(stream);
            dumper.Boolean(value, schema->as<node::Boolean>());
        } else if (schema->is<node::Null>()) {
            // std::cout << schema->getItemName() << ": null" << std::endl;
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