#ifndef _fileemitor_h
#define _fileemitor_h

#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include <avro/limiter.h>

namespace avro {
  class Reader;
  struct header;
  class BlockDecoder;
  class StringBuffer;
  namespace dumper {
     struct TsvExpression;
  }
}

namespace filter {
  class Filter;
}
struct Task {
    std::shared_ptr<avro::Reader> reader;
    std::shared_ptr<avro::header> header;
    std::shared_ptr<avro::BlockDecoder> decoder;
    std::shared_ptr<avro::StringBuffer> buffer;
    std::shared_ptr<avro::dumper::TsvExpression> tsvFieldsList;
    int64_t objectCount;
};

class FileEmitor {
public:

    FileEmitor(const std::vector<std::string> &fileList, int limit, std::function<void(const std::string&)> outDocument);

    std::shared_ptr<Task> getNextTask(std::unique_ptr<avro::BlockDecoder> &decoder, size_t &fileId);

    void enablePrintProcessingFile(); // TODO: use me
    void setFilter(std::shared_ptr<filter::Filter> filter); // TODO: use me
    void setTsvFieldList(const std::string &tsvFieldList); // TODO: use me

private:
    const std::vector<std::string> &fileList;
    bool printProcessingFile = false;
    size_t currentFile = 0;
    std::function<void(const std::string&)> outDocument;
    std::mutex ownLock;
    Task currentTaskSample;
    avro::Limiter limiter;
    std::shared_ptr<filter::Filter> filter;
    std::string tsvFieldList;
    bool stop = false;

    bool canProduceNextTask();
};

#endif