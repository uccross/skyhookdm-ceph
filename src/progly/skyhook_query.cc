#include "skyhook_query.h"

SkyhookQueryParser::SkyhookQueryParser(std::string &descr) {
    cli_opts = new opt_parser::options_description(descr);

    std::stringstream default_config;
    default_config << std::string(getenv("HOME")) << "/cluster/ceph.conf";

    cli_opts->add_options()
        (
            "help,h",
            "show help message"
        )
        (
            "pool",
            opt_parser::value<std::string>(),
            "Name of RADOS pool to query"
        )
        (
            "table",
            opt_parser::value<std::string>(),
            "Table prefix to use for object names (e.g. 'public.<table>.<suffix>')"
        )
        (
            "input-file",
            opt_parser::value<std::string>(),
            "Path to skyhook file to parse."
        )
        (
            "schema-file",
            opt_parser::value<std::string>(),
            "Path to file containing table schema as plaintext"
        )
        (
            "ceph-config",
            opt_parser::value<std::string>()->default_value(default_config.str()),
            "path to ceph.conf"
        )
    ;
}

opt_parser::variables_map *SkyhookQueryParser::parse_args(int argc, char **argv) {
    opt_parser::variables_map *parsed_args = new opt_parser::variables_map();
    opt_parser::store(opt_parser::parse_command_line(argc, argv, *cli_opts), *parsed_args);

    opt_parser::notify(*parsed_args);

    return parsed_args;
}
