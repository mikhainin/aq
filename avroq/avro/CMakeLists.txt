
aux_source_directory(node NODE_SOURCES)
aux_source_directory(codec CODEC_SOURCES)
aux_source_directory(predicate PREDICATE_SOURCES)

add_library(avro
                 blockdecoder.cc
                 exception.cc
                 deflatedbuffer.cc
                 filehandler.cc
                 limiter.cc
                 reader.cc
                 schemareader.cc
                 stringbuffer.cc

                 ${NODE_SOURCES}
                 ${CODEC_SOURCES}
                 ${PREDICATE_SOURCES}
)

