#include <memory>

#include <avro/block.h>
#include <avro/blockdecoder.h>
#include <avro/eof.h>
#include <avro/finished.h>

#include <filter/filter.h>

#include "fileemitor.h"

#include "worker.h"


Worker::Worker(FileEmitor &emitor) : emitor(emitor) {

}

void Worker::operator()() {

    avro::Block block;
    std::unique_ptr<avro::BlockDecoder> decoder;
    size_t fileId = -1;

    while (true) {

        auto task = emitor.getNextTask(decoder, fileId);

        if (!task) {
            break;
        }

        auto &buffer = *task->buffer;

        block.buffer.assignData(buffer.data(), buffer.size());
        block.objectCount = task->objectCount;

        try {
            decoder->decodeAndDumpBlock(block);
        } catch (const avro::Eof &e) {
            ; // reading done
        } catch (const avro::Finished &e) {
            break;
        }

    }
}
