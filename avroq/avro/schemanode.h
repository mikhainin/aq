#ifndef AVROQ_AVRO_SCHEMANODE_H_
#define AVROQ_AVRO_SCHEMANODE_H_

#include <string>

namespace avro {

class SchemaNode {
public:
    SchemaNode(const std::string &name);
    virtual ~SchemaNode();

    template<class T>
    inline
    bool is() {
        return dynamic_cast<T*>(this) != nullptr;
    }
private:
    std::string name;
};

}


#endif /* AVROQ_AVRO_SCHEMANODE_H_ */
