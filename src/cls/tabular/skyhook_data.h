#include <iostream>

#include "arrow/api.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/reader.h"
#include "flatbuffers/flatbuffers.h"

#include "fb_meta_generated.h"

const std::string* domain_binary_data_from_wrapper(Tables::FB_Meta *sky_wrapper);

/*
struct SkyhookWrapper {
    const Tables::FB_Meta *metadata = NULL;

    SkyhookWrapper(const char *flatbuffer_binary_data);

    uint64_t           get_domain_binary_size();
    const std::string* get_domain_binary_data();
};
*/

struct SkyhookDomainData {
    std::shared_ptr<arrow::Table>                  data_table;
    std::shared_ptr<arrow::Schema>                 data_schema;
    std::shared_ptr<const arrow::KeyValueMetadata> meta_schema;

    SkyhookDomainData(const std::string *binary_data_arrow);

    size_t num_rows();
    std::shared_ptr<arrow::ChunkedArray> get_column(const std::string &column_name);
};

