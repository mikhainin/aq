add_library(avro
				 reader.cc
				 schemareader.cc
                 stringbuffer.cc
                 deflatedbuffer.cc
                 limiter.cc
                 
# TODO: move to subfolder
                 node/node.cc
                 node/custom.cc
                 node/record.cc
                 node/string.cc
                 node/int.cc
                 node/float.cc
                 node/double.cc
                 node/union.cc
                 node/boolean.cc
                 node/null.cc
 )

