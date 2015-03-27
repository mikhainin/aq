/*
 * SchemaReader.h
 *
 *  Created on: 21 дек. 2014 г.
 *      Author: a
 */

#ifndef AVROQ_AVRO_SCHEMAREADER_H_
#define AVROQ_AVRO_SCHEMAREADER_H_

#include <boost/property_tree/ptree.hpp>

namespace avro {

class NodeDescriptor;
class RecordNode;
class SchemaNode;
class CustomType;
class EnumNode;

class SchemaReader {
public:
    SchemaReader(const std::string &schemaJson);
    virtual ~SchemaReader();

    std::unique_ptr<SchemaNode> parse();
private:
    const std::string &schemaJson;

    std::unique_ptr<SchemaNode> parseOneJsonObject(const boost::property_tree::ptree &node);
    std::unique_ptr<NodeDescriptor> descriptorForJsonObject(const boost::property_tree::ptree &node);

    void readRecordFields(const boost::property_tree::ptree &node, RecordNode &record);
    void readRecordDefinition(const boost::property_tree::ptree &node, CustomType &record);
    std::unique_ptr<EnumNode> readEnum(const boost::property_tree::ptree &node);
    std::unique_ptr<SchemaNode> nodeByType(const boost::property_tree::ptree  &node);
};

} /* namespace avro */

#endif /* AVROQ_AVRO_SCHEMAREADER_H_ */
