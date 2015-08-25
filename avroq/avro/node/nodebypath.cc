#include <iostream>

#include <boost/algorithm/string.hpp>

#include "node.h"
#include "custom.h"
#include "array.h"
#include "record.h"

namespace avro {
namespace node {

const Node *nodeByPath(const std::string &path, const Node *rootNode) {
    std::vector<std::string> chunks;
    boost::algorithm::split(chunks, path, boost::is_any_of("."));

    auto currentNode = rootNode;

    for(auto p = chunks.begin(); p != chunks.end(); ++p) {

        const Node *chunkItem = currentNode;

        if (chunkItem->is<node::Custom>()) {
            chunkItem = chunkItem->as<node::Custom>().getDefinition().get();
        }
        if (chunkItem->is<node::Record>()) {
            for( auto &n : chunkItem->as<node::Record>().getChildren()) {
                if (n->getItemName() == *p) {
                    chunkItem = n.get();
                    break;
                }
            }
        }

        if (chunkItem != currentNode) {
            currentNode = chunkItem;
            continue;
        }
        return nullptr;
    }

    return currentNode;
}

const Node* nodeByPathIgnoreArray(const std::string &path, const Node* rootNode) {

    std::vector<std::string> chunks;
    boost::algorithm::split(chunks, path, boost::is_any_of("."));

    auto currentNode = rootNode;

    for(auto p = chunks.begin(); p != chunks.end(); ++p) {

        const Node *chunkItem = currentNode;

        if (chunkItem->is<node::Custom>()) {
            chunkItem = chunkItem->as<node::Custom>().getDefinition().get();
        }
        if (chunkItem->is<node::Array>()) {
            chunkItem = chunkItem->as<node::Array>().getItemsType().get();
        }
        if (chunkItem->is<node::Record>()) {
            for( auto &n : chunkItem->as<node::Record>().getChildren()) {
                if (n->getItemName() == *p) {
                    chunkItem = n.get();
                    break;
                }
            }
        }
        std::cout << chunkItem->getItemName() << ':' << chunkItem->getTypeName() << std::endl;

        if (chunkItem != currentNode) {
            currentNode = chunkItem;
            continue;
        }
        return nullptr;
    }

    return currentNode;
}



}
}