#include <stdlib.h>
#include <iostream>
#include <fstream>

#include <boost/program_options.hpp>

namespace opt_parser = boost::program_options;

struct SkyhookQueryParser {
    opt_parser::options_description *cli_opts = NULL;

    SkyhookQueryParser(std::string &descr);

    opt_parser::variables_map *parse_args(int argc, char **argv);
};
