#include <functional>
#include <iostream>
#include <thread>
#include <mutex>

#include <boost/lambda/lambda.hpp>

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
    limiter(limit),
    countedDocuments(0) {

}

void FileEmitor::enablePrintProcessingFile() {
    printProcessingFile = true;
}

void FileEmitor::enableCountOnlyMode() {
    countMode = true;
}

void FileEmitor::setFilter(std::shared_ptr<filter::Filter> filter) {
    this->filter = filter;
}

void FileEmitor::setTsvFieldList(const std::string &tsvFieldList, const std::string &fieldSeparator) {
    this->tsvFieldList = tsvFieldList;
    this->fieldSeparator = fieldSeparator;
}


std::shared_ptr<Task> FileEmitor::getNextTask(
    std::unique_ptr<avro::BlockDecoder> &decoder,
    size_t &fileId) {

    std::lock_guard<std::mutex> lock(ownLock);
    if (stop) {
        return std::shared_ptr<Task>();
    }

    auto &currentFileName = fileList[currentFile];

    if (!currentTaskSample.reader || currentTaskSample.reader->eof()) {

        if (currentFile >= fileList.size()) {
            return std::shared_ptr<Task>();
        }

        if (printProcessingFile) {
            std::cerr << "Processing " << currentFileName << std::endl;
        }

        try{
            currentTaskSample.reader.reset(new avro::Reader(currentFileName));
        } catch (const std::runtime_error &e) {
            std::cerr << e.what() << std::endl;
            stop = true;
            return std::shared_ptr<Task>();
        }
        currentTaskSample.header.reset(
                new avro::header(currentTaskSample.reader->readHeader())
            );

        currentTaskSample.tsvFieldsList.reset(
                new avro::dumper::TsvExpression(
                    currentTaskSample.reader->compileFieldsList(
                            tsvFieldList,
                            *currentTaskSample.header,
                            fieldSeparator
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
            try {
                decoder->setFilter(
                        std::unique_ptr<filter::Filter>(
                            new filter::Filter(*filter)
                        )
                    );
            } catch (const std::runtime_error &e) {
                std::cerr << "Can't apply filter to file: " << currentFileName << std::endl
                    << e.what() << std::endl;
                stop = true;
            }
        }
        decoder->setTsvFilterExpression(*task->tsvFieldsList);

        decoder->setDumpMethod(outDocument);

        if (countMode) {
            using namespace std::placeholders;
            decoder->setCountMethod(
                std::bind(&FileEmitor::countDocument, this, _1)
                // countedDocuments += boost::lambda::_1
            );
            decoder->enableCountOnlyMode();
        }
    }

    task->buffer.reset(new avro::StringBuffer(
        currentTaskSample.reader->nextBlock(
            *task->header,
            task->objectCount
        )
    ));

    return task;
}

void FileEmitor::countDocument(size_t num) {
    (void)countedDocuments.fetch_add(num);
}

size_t FileEmitor::getCountedDocuments() const {
    return countedDocuments;
}
