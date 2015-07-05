add_library(avro
				 reader.cc
				 schemareader.cc
                 stringbuffer.cc
                 deflatedbuffer.cc
                 limiter.cc
                 filehandler.cc
                 
# TODO: move to subfolder
                 node/array.cc
                 node/boolean.cc
                 node/custom.cc
                 node/double.cc
                 node/enum.cc
                 node/float.cc
                 node/int.cc
                 node/long.cc
                 node/map.cc
                 node/node.cc
                 node/nodebypath.cc
                 node/null.cc
                 node/record.cc
                 node/string.cc
                 node/union.cc

 # TODO: move to subfolder
                 predicate/predicate.cc
)

