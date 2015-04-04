/*
 * SchemaReader.h
 *
 *  Created on: 21 ���. 2014 �.
 *      Author: a
 */

#ifndef AVROQ_AVRO_SCHEMAREADER_H_
#define AVROQ_AVRO_SCHEMAREADER_H_

#include <boost/property_tree/ptree.hpp>

namespace avro {

namespace node {
    class Record;
    class Union;
    class Custom;
}
class NodeDescriptor;
class SchemaNode;

class SchemaReader {
public:
    SchemaReader(const std::string &schemaJson);
    virtual ~SchemaReader();

    std::unique_ptr<SchemaNode> parse();

    void dumpSchema(std::unique_ptr<SchemaNode> schema, std::ostream &to);
private:
    const std::string &schemaJson;

    std::unique_ptr<SchemaNode> parseOneJsonObject(const boost::property_tree::ptree &node);
    std::unique_ptr<NodeDescriptor> descriptorForJsonObject(const boost::property_tree::ptree &node);

    void readRecordFields(const boost::property_tree::ptree &node, node::Record &record);
    void readRecordDefinition(const boost::property_tree::ptree &node, node::Custom &record);
    std::unique_ptr<node::Union> readUnion(const boost::property_tree::ptree &node, const std::string &name);
    
    std::unique_ptr<SchemaNode> nodeByType(const boost::property_tree::ptree &node,
                                           const std::string &nodeName);
};

} /* namespace avro */

#endif /* AVROQ_AVRO_SCHEMAREADER_H_ */
