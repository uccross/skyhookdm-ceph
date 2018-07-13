How to Use the Client App to Flush Data to Ceph
-----------------------------------------------

Requirements
1. Install Ceph dependencies/libraries
2. Install FlatBuffers library including the flatc compiler
	git clone https://github.com/google/flatbuffers
3. Client app and schema files from SkyHookDB repository
	https://github.com/uccross/skyhook-ceph/tree/skyhook-kraken/src/progly/flatbuffers/read_write_to_and_from_disk
4. Cmake to generate our flatc compiler

Instructions
1. Get a Ceph cluster working
2. Use the .fbs schema file in the SkyHookDB repo or create your own schema file using the tutorial found at the following link:
	https://google.github.io/flatbuffers/flatbuffers_guide_tutorial.html
3. Compile the flatc.exe by following these instructions
	-git clone https://github.com/google/flatbuffers
	-cd flatbuffers
	-cmake -G "Unix Makefiles"
	-make
	-./flattests #this should print "ALL TESTS PASSED
	- now we have our ./flatc compiler
4. Compile the .fbs schema file using flatc compiler
	- ./flatc -c "file_name.fbs"
	- This will generate a "file_name_generated.h"
5. Include the header files in the client app
	-"header_files/flatbuffers/flexbuffers.h"
	-"..._generated.h" in our case ("skyhookv1_generated.h")
6. Compile client app "writeBuffer.cpp"
	- use the following command:
		g++ -W -Wall -std=c++11 writeBuffer.cpp -o writeBuffer
7. Use command line options or -h for info
	./writeBuffer -h
	./writeBuffer -f lineitem-10K-rows.tbl -s lineitem_schema.txt -o 39 -r 10 -n 50

