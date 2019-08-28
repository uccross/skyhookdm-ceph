/*
* Copyright (C) 2018 The Regents of the University of California
* All Rights Reserved
*
* This library can redistribute it and/or modify under the terms
* of the GNU Lesser General Public License Version 2.1 as published
* by the Free Software Foundation.
*
*/

#include "fbwriter_fbu_utils.h"

namespace po = boost::program_options ;

int main( int argc, char *argv[] ) {

  bool debug ;
  std::string write_type ;
  std::string filename ;
  std::string schema_datatypes ;
  std::string schema_attnames ;
  std::string schema_iskey ;
  std::string schema_isnullable ;
  std::string table_name ;
  uint64_t nrows ;
  uint64_t ncols ;
  uint64_t cols_per_fb ;
  std::string writeto ;
  std::string targetformat ;
  std::string targetoid ;
  std::string targetpool ;
  std::string savedir ;
  std::uint64_t rid_start_value;
  std::uint64_t rid_end_value;
  std::uint64_t obj_counter ;

  po::options_description gen_opts("General options");
  gen_opts.add_options()
    ("help,h", "show help message")
    ("debug", po::value<bool>(&debug)->required(), "debug")
    ("write_type", po::value<std::string>(&write_type)->required(), "write_type")
    ("filename", po::value<std::string>(&filename)->required(), "filename")
    ("schema_datatypes", po::value<std::string>(&schema_datatypes)->required(), "schema_datatypes")
    ("schema_attnames", po::value<std::string>(&schema_attnames)->required(), "schema_attnames")
    ("schema_iskey", po::value<std::string>(&schema_iskey)->required(), "schema_iskey")
    ("schema_isnullable", po::value<std::string>(&schema_isnullable)->required(), "schema_isnullable")
    ("table_name", po::value<std::string>(&table_name)->required(), "table_name")
    ("nrows", po::value<uint64_t>(&nrows)->required(), "nrows")
    ("ncols", po::value<uint64_t>(&ncols)->required(), "ncols")
    ("cols_per_fb", po::value<uint64_t>(&cols_per_fb), "cols_per_fb")
    ("writeto", po::value<std::string>(&writeto)->required(), "writeto")
    ("targetformat", po::value<std::string>(&targetformat)->required(), "targetformat")
    ("targetoid", po::value<std::string>(&targetoid)->required(), "targetoid")
    ("targetpool", po::value<std::string>(&targetpool)->required(), "targetpool")
    ("savedir", po::value<std::string>(&savedir)->required(), "savedir")
    ("rid_start_value", po::value<uint64_t>(&rid_start_value)->required(), "rid_start_value")
    ("rid_end_value", po::value<uint64_t>(&rid_end_value)->required(), "rid_end_value")
    ("obj_counter", po::value<uint64_t>(&obj_counter)->required(), "obj_counter") ;

  po::options_description all_opts( "Allowed options" ) ;
  all_opts.add( gen_opts ) ;
  po::variables_map vm ;
  po::store( po::parse_command_line( argc, argv, all_opts ), vm ) ;
  if( vm.count( "help" ) ) {
    std::cout << all_opts << std::endl ;
    return 1;
  }
  po::notify( vm ) ;

  // sanity checks
  assert( rid_start_value < nrows ) ;
  assert( rid_end_value <= nrows ) ;
  assert( rid_start_value <= rid_end_value ) ;

  cmdline_inputs_t inputs ;
  inputs.debug             = debug ;
  inputs.write_type        = write_type ;
  inputs.filename          = filename ;
  inputs.schema_datatypes  = schema_datatypes ;
  inputs.schema_attnames   = schema_attnames ;
  inputs.schema_iskey      = schema_iskey ;
  inputs.schema_isnullable = schema_isnullable ;
  inputs.table_name        = table_name ;
  inputs.nrows             = nrows ;
  inputs.ncols             = ncols ;
  inputs.writeto           = writeto ;
  inputs.targetformat      = targetformat ;
  inputs.targetoid         = targetoid ;
  inputs.targetpool        = targetpool ;
  inputs.cols_per_fb       = cols_per_fb ;
  inputs.obj_counter       = obj_counter ;

  do_write( inputs, rid_start_value, rid_end_value, debug, savedir ) ;

  return 0 ;
} // main
