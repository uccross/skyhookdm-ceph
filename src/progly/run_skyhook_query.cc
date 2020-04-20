#include "include/rados/librados.hpp"

#include "skyhook_query.h"
#include "../cls/tabular/skyhook_data.h"

#define DEBUG 1

#define STATUS_PRINT_HELP     1
#define STATUS_WHAT_DO        2
#define STATUS_ERROR_RESOURCE 3
#define STATUS_ERROR_EXEC     4

/*
 * Takes an opened input file stream and: (1) Seeks to the end, (2) Asks for the
 * position (byte-based), (3) Seeks back to the beginning.
 */
size_t size_in_bytes(std::ifstream *input_file_stream) {
    if (not input_file_stream->is_open()) { return 0; }

    input_file_stream->seekg(0, std::ios::end);
    size_t byte_count = input_file_stream->tellg();
    input_file_stream->seekg(0, std::ios::beg);

    return byte_count;
}

/**
 * Opens and reads file data into a char array ("raw bytes"). The file handle is
 * closed when the std::ifstream goes out of scope and is deconstructed.
 */
char* bytebuffer_from_filepath(std::string &path_to_input) {
    std::cout << "[INFO] Parsing file: " << path_to_input << std::endl;

    std::ifstream input_file_stream(path_to_input, std::ios::in | std::ios::binary);
    if (not input_file_stream.is_open()) {
        std::cerr << "Unable to open file: " << path_to_input << std::endl;
        return NULL;
    }

    // Prep for reading file data into the char buffer
    size_t input_byte_count = size_in_bytes(&input_file_stream);
    char *data              = new char[input_byte_count];

    input_file_stream.read(data, input_byte_count);

    return data;
}

int config_cluster_handle(opt_parser::variables_map *cli_args, librados::Rados *cluster) {
    std::string path_to_config;

    try {
        path_to_config = (*cli_args)["ceph-config"].as<std::string>();
    }

    catch (const boost::bad_any_cast &ex) {
        return STATUS_PRINT_HELP;
    }

    cluster->init(NULL);
    cluster->conf_read_file(path_to_config.c_str());

    return cluster->connect();
}

int query_cluster(opt_parser::variables_map *cli_args, SkyhookWrapper **sky_wrapper) {
    // ------------------------------
    // Grab variables from CLI arguments
    std::string pool_name, table_name, path_to_config;

    try {
        pool_name      = (*cli_args)["pool"].as<std::string>();
        table_name     = (*cli_args)["table"].as<std::string>();
    }

    catch (const boost::bad_any_cast &ex) {
        return STATUS_PRINT_HELP;
    }

    // ------------------------------
    // Cluster interactions
    librados::Rados cluster;
    librados::IoCtx io_context;
    int skyhook_status = 0;

    // configure and connect
    skyhook_status = config_cluster_handle(cli_args, &cluster);
    if (skyhook_status < 0) { return STATUS_ERROR_RESOURCE; }

    // create an I/O context
    skyhook_status = cluster.ioctx_create(pool_name.c_str(), io_context);
    if (skyhook_status < 0) { return STATUS_ERROR_RESOURCE; }

    // issue read request
    std::stringstream obj_name;
    obj_name << "public." << table_name << ".0";

    librados::bufferlist    bufferlist_read;
    librados::AioCompletion *read_completion = librados::Rados::aio_create_completion();

    skyhook_status = io_context.aio_read(obj_name.str(), read_completion, &bufferlist_read, 0, 0);
    if (skyhook_status < 0) { return STATUS_ERROR_EXEC; }

    // Wait for request completion and check status
    read_completion->wait_for_complete();

    skyhook_status = read_completion->get_return_value();
    if (skyhook_status < 0) { return STATUS_ERROR_EXEC; }

    // Prep sky_wrapper with read data
    // char *object_data = new char[bufferlist_read.length() + 1];
    // memcpy(object_data, bufferlist_read.c_str(), bufferlist_read.length() + 1);

    // *sky_wrapper = (Tables::FB_Meta *) Tables::GetFB_Meta(object_data);
    *sky_wrapper = new SkyhookWrapper(&bufferlist_read);
    return 0;
};

int parse_file(opt_parser::variables_map *cli_args, SkyhookWrapper **sky_wrapper) {
    std::string path_to_datafile;

    try {
        path_to_datafile = (*cli_args)["input-file"].as<std::string>();
    }

    catch (const boost::bad_any_cast &ex) {
        return STATUS_PRINT_HELP;
    }

    const char *file_data = bytebuffer_from_filepath(path_to_datafile);
    if (file_data == NULL) { return STATUS_ERROR_RESOURCE; }

    *sky_wrapper = new SkyhookWrapper(file_data);
    // *sky_wrapper = (Tables::FB_Meta *) Tables::GetFB_Meta(file_data);
    return 0;
}

int main(int argc, char **argv) {
    // ------------------------------
    // Parse CLI Options
    std::string program_descr = "Skyhook query command-line interface";

    SkyhookQueryParser query_cli_parser(program_descr);
    opt_parser::variables_map *parsed_args = query_cli_parser.parse_args(argc, argv);

    if (parsed_args->count("help")) {
        std::cout << *(query_cli_parser.cli_opts) << std::endl;
        return 0;
    }

    // ------------------------------
    // Do work

    int status_code = STATUS_WHAT_DO;

    SkyhookWrapper *sky_wrapper = NULL;

    if (parsed_args->count("pool")) {
        status_code = query_cluster(parsed_args, &sky_wrapper);
    }

    else if (parsed_args->count("input-file")) {
        status_code = parse_file(parsed_args, &sky_wrapper);
    }

    if (status_code == 0) {
        SkyhookDomainData *sky_domain = new SkyhookDomainData(sky_wrapper->get_domain_binary_data());

        std::cout << "[ARROW] Table |> Row Count: " << sky_domain->num_rows()
                  << std::endl
                  << "[ARROW] Table |> Schema: "    << sky_domain->data_schema->ToString()
                  << std::endl;

        std::string column_name = "0000892fbc276097f396352e3f2a4b51";
        std::shared_ptr<arrow::ChunkedArray> table_column = sky_domain->get_column(column_name);
        std::shared_ptr<arrow::Array>        column_chunk = table_column->chunk(0);

        std::cout << "[ARROW] Table |> Column: "      << column_name
                  << std::endl
                  << "[ARROW] Table |> Chunk Count: " << table_column->num_chunks()
                  << std::endl
                  << "[ARROW] Chunk |> Data: "        << column_chunk->ToString()
                  << std::endl;
    }

    // ------------------------------
    // Return status based on what was done

    // This means we didn't do anything, not even print help.
    if (status_code == STATUS_WHAT_DO) {
        // Suggest a run mode (how to meaningfully use this program)
        std::cerr << "Please specify an input file." << std::endl;
    }

    // This means that whatever we did, it failed to start
    else if (status_code == STATUS_PRINT_HELP) {
        // Print the help message, because an input was probably borked
        std::cout << *(query_cli_parser.cli_opts) << std::endl;
    }

    return status_code;
}
