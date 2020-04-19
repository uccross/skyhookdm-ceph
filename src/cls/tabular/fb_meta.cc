#include <iostream>
#include <fstream>
#include <sstream>

#include "include/rados/librados.hpp"

#include "arrow/api.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/reader.h"
#include "flatbuffers/flatbuffers.h"

#include "fb_meta_generated.h"

size_t size_in_bytes(std::ifstream *input_file_stream) {
    /*
     * Convenience function that takes an opened input file stream and:
     * - Seeks to the end
     * - Asks for the position (byte-based)
     * - Seeks to the beginning (for follow-up functions)
     */

    if (not input_file_stream->is_open()) { return 0; }

    input_file_stream->seekg(0, std::ios::end);
    size_t byte_count = input_file_stream->tellg();
    input_file_stream->seekg(0, std::ios::beg);

    return byte_count;
}

char* bytebuffer_from_skyhook_obj() {
    /**
     * Opens file, <path_to_input>, and reads data into a char array (for use with arrow
     * functions). The opened file is closed when the input file stream (std::ifstream) goes out of
     * scope and is deconstructed.
     */

    int skyhook_status = 0;

    std::stringstream default_config;
    default_config << std::string(getenv("HOME")) << "/cluster/ceph.conf";

    // ------------------------------
    // Cluster interactions

    // configure and connect
    librados::Rados cluster;
    cluster.init(NULL);
    cluster.conf_read_file(default_config.str().c_str());

    skyhook_status = cluster.connect();
    if (skyhook_status < 0) { std::cerr << "FAIL" << std::endl; }

    // create an I/O context
    librados::IoCtx io_context;
    skyhook_status = cluster.ioctx_create("single-cell", io_context);
    if (skyhook_status < 0) { std::cerr << "FAIL" << std::endl; }

    // issue read request
    librados::bufferlist    bufferlist_read;
    librados::AioCompletion *read_completion = librados::Rados::aio_create_completion();

    skyhook_status = io_context.aio_read("public.immune.0", read_completion, &bufferlist_read, 0, 0);
    if (skyhook_status < 0) { std::cerr << "FAIL" << std::endl; }

    // Wait for request completion and check status
    read_completion->wait_for_complete();

    skyhook_status = read_completion->get_return_value();
    if (skyhook_status < 0) { std::cerr << "FAIL" << std::endl; }

    // Prep sky_wrapper with read data
    std::cout << "Read completed and successful" << std::endl;

    char *buffer = new char[bufferlist_read.length() + 1];
    memcpy(buffer, bufferlist_read.c_str(), bufferlist_read.length() + 1);

    return buffer;
}

int table_from_record_reader(std::shared_ptr<arrow::ipc::RecordBatchReader> &record_reader,
                             std::shared_ptr<arrow::Table>                  &arrow_table) {

    std::vector<std::shared_ptr<arrow::RecordBatch>> accumulated_records;
    std::shared_ptr<arrow::RecordBatch>              current_record;

    // Iterate over RecordBatch objects using `record_reader` into an arrow Table
    while (true) {
        record_reader->ReadNext(&current_record);
        if (current_record == nullptr) { break; }

        accumulated_records.push_back(current_record);
    }

    arrow::Table::FromRecordBatches(accumulated_records, &arrow_table);

    return 0;
}

void parse_skyhook_format(const char *file_data) {
    std::shared_ptr<arrow::Buffer>                 arrow_buffer;
    std::shared_ptr<arrow::Table>                  arrow_table;
    std::shared_ptr<arrow::ipc::RecordBatchReader> arrow_record_reader;

    auto flatbuf_wrapper = Tables::GetFB_Meta(file_data);

    // Temporary Debug statements of some flatbuffer fields
    std::cout << "[FLATBUF] Blob Format: " << flatbuf_wrapper->blob_format()
              << std::endl
              << "[FLATBUF] Blob Size: "   << flatbuf_wrapper->blob_size()
              << std::endl;

    // Access the data from the flatbuffer metadata wrapper
    const flatbuffers::Vector<uint8_t> *stored_data_blob = flatbuf_wrapper->blob_data();

    // Create an arrow Buffer from a raw data string (constructed from raw storage blob)
    const std::string raw_buffer_data = std::string(
        reinterpret_cast<const char *>(stored_data_blob->data()),
        flatbuf_wrapper->blob_size()
    );
    arrow::Buffer::FromString(raw_buffer_data, &arrow_buffer);

    // Prepare a Buffer Reader (input stream) and RecordBatch Reader to stream over the arrow Buffer
    const std::shared_ptr<arrow::io::BufferReader> buffer_reader = std::make_shared<arrow::io::BufferReader>(arrow_buffer);
    arrow::ipc::RecordBatchStreamReader::Open(buffer_reader, &arrow_record_reader);

    // Validate that all is well by first grabbing the schema and metadata
    auto schema   = arrow_record_reader->schema();
    auto metadata = schema->metadata();

    std::cout << "[ARROW] Table Schema: "  << schema->ToString() << std::endl;
    // std::cout << "[ARROW Table Metadata: " << metadata << std::endl;

    // Read from `arrow_record_reader` and parse into `arrow_table`
    int parse_status = table_from_record_reader(arrow_record_reader, arrow_table);
    if (parse_status) {
        std::cerr << "[Error] Error when parsing arrow buffer into Table: "
                  << parse_status
                  << std::endl;
    }

    std::cout << "[ARROW] Table |> Row Count: " << arrow_table->num_rows()
              << std::endl;

    std::string column_name = "0000892fbc276097f396352e3f2a4b51";
    auto table_column = arrow_table->GetColumnByName(column_name);
    auto column_chunk = table_column->chunk(0);

    std::cout << "[ARROW] Table |> Column: "      << column_name
              << std::endl
              << "[ARROW] Table |> Chunk Count: " << table_column->num_chunks()
              << std::endl
              << "[ARROW] Chunk |> Data: "        << column_chunk->ToString()
              << std::endl;
}

int main (int argc, char **argv) {
    const char *object_data = bytebuffer_from_skyhook_obj();

    // *sky_wrapper = (Tables::FB_Meta *) Tables::GetFB_Meta(object_data);

    // Pass file data to be parsed as a skyhook flatbuffer metadata wrapper
    parse_skyhook_format(object_data);

    return 0;
}
