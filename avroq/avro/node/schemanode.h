#ifndef AVROQ_AVRO_SCHEMANODE_H_
#define AVROQ_AVRO_SCHEMANODE_H_

#include <string>

namespace avro {

class SchemaNode {
public:
    SchemaNode(const std::string &typeName, const std::string &itemName);
    virtual ~SchemaNode();

    template<class T>
    inline bool is() {
        return dynamic_cast<T*>(this) != nullptr;
    }

    template<class T>
    inline T& as() {
        return dynamic_cast<T&>(*this);
    }

    const std::string &getTypeName() const;

    const std::string &getItemName() const;
    //virtual void dump(std::function<void(std::string)> &dumper) = 0;

private:
    std::string typeName;
    std::string itemName;
};

}


#endif /* AVROQ_AVRO_SCHEMANODE_H_ */
