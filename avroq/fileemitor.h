#ifndef _fileemitor_h
#define _fileemitor_h

#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include <avro/limiter.h>

#include <util/concurrentqueue.hpp>


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
    std::shared_ptr<avro::StringBuffer> buffer;
    std::shared_ptr<avro::dumper::TsvExpression> tsvFieldsList;
    int64_t objectCount;
    size_t fileId;
    std::string currentFileName;
};

class FileEmitor {
public:

    FileEmitor(const std::vector<std::string> &fileList, int limit, std::function<void(const std::string&)> outDocument);

    std::shared_ptr<Task> getNextTask(std::unique_ptr<avro::BlockDecoder> &decoder, size_t &fileId);

    void enablePrintProcessingFile();
    void enableCountOnlyMode();
    void enableParseLoop();
    void setFilter(std::shared_ptr<filter::Filter> filter);
    void setTsvFieldList(const std::string &tsvFieldList, const std::string &fieldSeparator);
    void finished();

    size_t getCountedDocuments() const;

    const std::string &getLastError() const;
    void outputAsJson(bool pretty);

    void operator()();

private:
    const std::vector<std::string> &fileList;
    std::string fieldSeparator;
    bool printProcessingFile = false;
    size_t currentFile = 0;
    std::function<void(const std::string&)> outDocument;
    std::mutex ownLock;
    Task currentTaskSample;
    avro::Limiter limiter;
    std::shared_ptr<filter::Filter> filter;
    std::string tsvFieldList;
    std::atomic_size_t countedDocuments;
    bool stop = false;
    bool countMode = false;
    bool parseLoopEnabled = false;
    bool jsonMode = false;
    bool jsonPrettyMode = false;
    std::string lastError;

    util::conqurrent_queue<std::shared_ptr<Task>> queue;

    bool canProduceNextTask();
    void mainLoop();

    void countDocument(size_t num);

    std::shared_ptr<Task> returnStop(const std::string &reason);
};

#endif
