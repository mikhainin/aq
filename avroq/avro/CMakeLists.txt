add_library(avro
				 reader.cc
				 schemareader.cc
                 
# TODO: move to subfolder
                 node/schemanode.cc
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

