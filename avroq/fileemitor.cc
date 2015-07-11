#include <iostream>
#include <thread>
#include <mutex>

#include <avro/node/all_nodes.h>
#include <avro/blockdecoder.h>
#include <avro/header.h>
#include <avro/reader.h>

#include <filter/filter.h>

#include "fileemitor.h"



FileEmitor::FileEmitor( const std::vector<std::string> &fileList,
                        int limit,
                        std::function<void(const std::string&)> outDocument) :
    fileList(fileList),
    outDocument(outDocument),
    limiter(limit) {

}

void FileEmitor::enablePrintProcessingFile() {
    printProcessingFile = true;
}

void FileEmitor::setFilter(std::shared_ptr<filter::Filter> filter) {
    this->filter = filter;
}

void FileEmitor::setTsvFieldList(const std::string &tsvFieldList) {
    this->tsvFieldList = tsvFieldList;
}


std::shared_ptr<Task> FileEmitor::getNextTask(
    std::unique_ptr<avro::BlockDecoder> &decoder,
    size_t &fileId) {

    std::lock_guard<std::mutex> lock(ownLock);

    if (!currentTaskSample.reader || currentTaskSample.reader->eof()) {

        if (currentFile >= fileList.size()) {
            return std::shared_ptr<Task>();
        }

        auto &p = fileList[currentFile];

        if (printProcessingFile) {
            std::cerr << "Processing " << p << std::endl;
        }

        currentTaskSample.reader.reset(new avro::Reader(p));
        currentTaskSample.header.reset(
                new avro::header(currentTaskSample.reader->readHeader())
            );

        currentTaskSample.tsvFieldsList.reset(
                new avro::dumper::TsvExpression(
                    currentTaskSample.reader->compileFieldsList(
                            tsvFieldList,
                            *currentTaskSample.header
                        )
                )
            );


        ++currentFile;

    }

    std::shared_ptr<Task> task(new Task(currentTaskSample));

    if (!decoder || fileId != currentFile) {

        fileId = currentFile;

        decoder.reset(
                new avro::BlockDecoder(
                    *currentTaskSample.header,
                    limiter
                )
            );
        if (filter) {
            decoder->setFilter(
                    std::unique_ptr<filter::Filter>(
                        new filter::Filter(*filter)
                    )
                );
        }
        decoder->setTsvFilterExpression(*currentTaskSample.tsvFieldsList);

        decoder->setDumpMethod(outDocument);

    }

    task->buffer.reset(new avro::StringBuffer(
        currentTaskSample.reader->nextBlock(
            *task->header,
            task->objectCount
        )
    ));

    return task;
}

