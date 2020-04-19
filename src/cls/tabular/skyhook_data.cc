#include "skyhook_data.h"

#define DEBUG 1

// ------------------------------
// Convenience functions
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

    if (DEBUG) { std::cout << "Constructing arrow::Table from RecordBatches" << std::endl; }
    arrow::Table::FromRecordBatches(accumulated_records, &arrow_table);

    return 0;
}

const std::string* domain_binary_data_from_wrapper(Tables::FB_Meta *sky_wrapper) {
    // Get the binary data from the flatbuffer structure
    const flatbuffers::Vector<uint8_t> *blob_data_binary = sky_wrapper->blob_data();

    // Then, get the raw data point out of the Vector to construct a std::string
    // TODO: see if there's a better (aka more readable) way to convert
    const std::string *raw_buffer_data = new std::string(
        (const char *)(blob_data_binary->data()),
        sky_wrapper->blob_size()
    );

    return raw_buffer_data;
}

// ------------------------------
// SkyhookWrapper member functions
/*
SkyhookWrapper::SkyhookWrapper(const char *skyhook_meta_flatbuffer) {
    metadata = Tables::GetFB_Meta(skyhook_meta_flatbuffer);

    // Temporary Debug statements of some flatbuffer fields
    std::cout << "[FLATBUF] Blob Format: " << metadata->blob_format()
              << std::endl
              << "[FLATBUF] Blob Size: "   << metadata->blob_size()
              << std::endl;
}

uint64_t SkyhookWrapper::get_domain_binary_size() {
    return metadata->blob_size();
}

const std::string* SkyhookWrapper::get_domain_binary_data() {
    // Get the binary data from the flatbuffer structure
    const flatbuffers::Vector<uint8_t> *blob_data_binary = metadata->blob_data();

    // Then, get the raw data point out of the Vector to construct a std::string
    // TODO: see if there's a better (aka more readable) way to convert
    const std::string *raw_buffer_data = new std::string(
        (const char *)(blob_data_binary->data()),
        metadata->blob_size()
    );

    return raw_buffer_data;
}
*/

// ------------------------------
// SkyhookDomainData member functions
SkyhookDomainData::SkyhookDomainData(const std::string *binary_data_arrow) {
    std::shared_ptr<arrow::Buffer>                 arrow_buffer;
    std::shared_ptr<arrow::ipc::RecordBatchReader> arrow_record_reader;

    // Create an arrow::Buffer
    if (DEBUG) { std::cout << "Creating arrow::Buffer" << std::endl; }
    arrow::Buffer::FromString(*(binary_data_arrow), &arrow_buffer);

    // Prepare a Buffer Reader (input stream) and RecordBatch Reader to stream over the arrow Buffer
    if (DEBUG) { std::cout << "Creating arrow::BufferReader" << std::endl; }
    const std::shared_ptr<arrow::io::BufferReader> buffer_reader = std::make_shared<arrow::io::BufferReader>(arrow_buffer);

    if (DEBUG) { std::cout << "Open arrow buffer with RecordBatchStreamReader" << std::endl; }
    arrow::ipc::RecordBatchStreamReader::Open(buffer_reader, &arrow_record_reader);

    // Read `arrow_record_reader` objects into `data_table` (member variable)
    int parse_status = table_from_record_reader(arrow_record_reader, data_table);

    // Data successfully parsed
    if (parse_status == 0) {
        // Set remaining member variables
        data_schema = arrow_record_reader->schema();
        meta_schema = data_schema->metadata();
    }

    else {
        std::cerr << "[Error] Could not parse arrow buffer into Table: "
                  << std::endl;
    }
}

size_t SkyhookDomainData::num_rows() {
    return data_table->num_rows();
}

std::shared_ptr<arrow::ChunkedArray> SkyhookDomainData::get_column(const std::string &column_name) {
    return data_table->GetColumnByName(column_name);
}
