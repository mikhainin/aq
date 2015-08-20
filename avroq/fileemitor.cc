#include <functional>
#include <iostream>
#include <thread>
#include <mutex>

#include <boost/lambda/lambda.hpp>

#include <avro/node/all_nodes.h>
#include <avro/blockdecoder.h>
#include <avro/exception.h>
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

void FileEmitor::enableParseLoop() {
    parseLoopEnabled = true;
}

std::shared_ptr<Task> FileEmitor::getNextTask(
    std::unique_ptr<avro::BlockDecoder> &decoder,
    size_t &fileId) {
/*
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
			return returnStop("Can't apply TSV expression to file: " + currentFileName + "\n"
							  "path '" + e.getPath() + "' was not found");
		} catch (const std::runtime_error &e) {
			return returnStop("Can't apply TSV expression to file: " + currentFileName + "\n" + e.what());
		}


        ++currentFile;

    }
*/
    std::shared_ptr<Task> task;// (new Task(currentTaskSample));

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
                return returnStop("Can't apply filter to file: " + task->currentFileName + e.what());
            }
        }
        try {
            decoder->setTsvFilterExpression(*task->tsvFieldsList);
        } catch (const avro::PathNotFound &e) {
            return returnStop("Can't apply TSV expression to file: " + task->currentFileName + "\n"
                              "path '" + e.getPath() + "' was not found");
        } catch (const std::runtime_error &e) {
            return returnStop("Can't apply TSV expression to file: " + task->currentFileName + "\n" + e.what());
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

    task->buffer.reset(new avro::StringBuffer(
        task->reader->nextBlock(
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

std::shared_ptr<Task> FileEmitor::returnStop(const std::string &reason) {
    lastError = reason;
    stop = true;
    queue.done();
    return std::shared_ptr<Task>();
}

const std::string &FileEmitor::getLastError() const {
    return lastError;
}

void FileEmitor::operator()() {

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
            queue.done();
            break;
        }
        currentTaskSample.header.reset(
                new avro::header(currentTaskSample.reader->readHeader())
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
            queue.done();
			//returnStop("Can't apply TSV expression to file: " + currentFileName + "\n"
			//				  "path '" + e.getPath() + "' was not found");
		} catch (const std::runtime_error &e) {
            stop = true;
            queue.done();
			// returnStop("Can't apply TSV expression to file: " + currentFileName + "\n" + e.what());
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

			queue.push(task);
		}

		++currentFileId;
	}
}
