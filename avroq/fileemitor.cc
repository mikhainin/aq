#include <functional>
#include <iostream>
#include <thread>
#include <mutex>

#include <boost/lambda/lambda.hpp>

#include <util/onscopeexit.h>

#include <avro/node/all_nodes.h>
#include <avro/blockdecoder.h>
#include <avro/exception.h>
#include <avro/header.h>
#include <avro/reader.h>

#include <filter/filter.h>
#include <filter/equality_expression.h>
#include <filter/record_expression.h>

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

void FileEmitor::enableParseLoop() {
    parseLoopEnabled = true;
}

void FileEmitor::outputAsJson(bool pretty) {
    jsonMode = true;
    jsonPrettyMode = pretty;
}

std::shared_ptr<Task> FileEmitor::getNextTask(
    std::unique_ptr<avro::BlockDecoder> &decoder,
    size_t &fileId) {

    std::shared_ptr<Task> task;

    if (!queue.pop(task)) {
    	return std::shared_ptr<Task>();
    }
    if (!decoder || fileId != task->fileId) {

        fileId = task->fileId;

        decoder.reset(
                new avro::BlockDecoder(
                    *task->header,
                    limiter
                )
            );
        if (parseLoopEnabled) {
            decoder->enableParseLoop();
        }
        if (jsonMode) {
            decoder->outputAsJson(jsonPrettyMode);
        }
        if (filter) {
            try {
                decoder->setFilter(
                        std::unique_ptr<filter::Filter>(
                            new filter::Filter(*filter)
                        )
                    );
            } catch (const avro::PathNotFound &e) {
                return returnStop("Can't apply filter to file: " + task->currentFileName + "\n"
                                  "path '" + e.getPath() + "' was not found");
            } catch (const std::runtime_error &e) {
                return returnStop("Can't apply filter to file: " + task->currentFileName +  ":" + e.what());
            }
        }
        try {
            decoder->setTsvFilterExpression(*task->tsvFieldsList);
        } catch (const avro::PathNotFound &e) {
            return returnStop("Can't apply TSV expression to file: " + task->currentFileName + "\n"
                              "path '" + e.getPath() + "' was not found");
        } catch (const std::runtime_error &e) {
            return returnStop("Can't apply TSV expression to file: " + task->currentFileName + ": " + e.what());
        }

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

    return task;
}

void FileEmitor::countDocument(size_t num) {
    (void)countedDocuments.fetch_add(num);
}

size_t FileEmitor::getCountedDocuments() const {
    return countedDocuments;
}

std::shared_ptr<Task> FileEmitor::returnStop(const std::string &reason) {
    lastError = reason;
    stop = true;
    queue.done();
    return std::shared_ptr<Task>();
}

void FileEmitor::finished() {
    stop = true;
    queue.done();
}

const std::string &FileEmitor::getLastError() const {
    return lastError;
}

void FileEmitor::operator()() {
	try {
		mainLoop();
	} catch(const std::runtime_error &e) {
		// TODO: stop processing completely
		stop = true;
		std::cerr << "Ooops! Something happened: " << e.what() << std::endl;
	}
}

void FileEmitor::mainLoop() {

       util::on_scope_exit dothis([this]() {
                       queue.done();
               });

	size_t currentFileId = 0;
	for(auto const &currentFileName : fileList) {

        if (printProcessingFile) {
            std::cerr << "Processing " << currentFileName << std::endl;
        }

        try{
            currentTaskSample.reader.reset(new avro::Reader(currentFileName));
        } catch (const std::runtime_error &e) {
            std::cerr << e.what() << std::endl;
            stop = true;
            break;
        }
        auto newHeader = currentTaskSample.reader->readHeader();
        if (currentTaskSample.header && newHeader == *currentTaskSample.header) {
            currentTaskSample.header->setSync(newHeader.sync);
        } else {

            ++currentFileId;

            currentTaskSample.reader->parseSchema(newHeader);

            currentTaskSample.header.reset(
                    new avro::header(std::move(newHeader))
                );

            try {
                currentTaskSample.tsvFieldsList.reset(
                        new avro::dumper::TsvExpression(
                            currentTaskSample.reader->compileFieldsList(
                                    tsvFieldList,
                                    *currentTaskSample.header,
                                    fieldSeparator
                                )
                        )
                    );
            } catch (const avro::PathNotFound &e) {
                stop = true;
                lastError = "Can't apply TSV expression to file: " + currentFileName + "\n"
                                             "path '" + e.getPath() + "' was not found";
                return;
            } catch (const std::runtime_error &e) {
                stop = true;
                lastError = "Can't apply TSV expression to file: " + currentFileName + "\n" + e.what();

                return;
            }

        }
		currentTaskSample.currentFileName = currentFileName;

		while(!currentTaskSample.reader->eof()) {

		    std::shared_ptr<Task> task(new Task(currentTaskSample));

		    task->fileId = currentFileId;

		    task->buffer.reset(new avro::StringBuffer(
		        task->reader->nextBlock(
		            *task->header,
		            task->objectCount
		        )
		    ));

		    if(!queue.push(task)) {
                        return;
                    }
		}

	}
}
