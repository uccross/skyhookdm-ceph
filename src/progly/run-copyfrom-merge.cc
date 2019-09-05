/*
* Copyright (C) 2018 The Regents of the University of California
* All Rights Reserved
*
* This library can redistribute it and/or modify under the terms
* of the GNU Lesser General Public License Version 2.1 as published
* by the Free Software Foundation.
*
*/

#include <fstream>
#include <boost/program_options.hpp>
#include "include/rados/librados.hpp"
#include "cls/tabular/cls_tabular_utils.h"
#include "cls/tabular/cls_tabular.h"

namespace po = boost::program_options;

int main(int argc, char **argv)
{
  std::string pool;
  std::string obj_prefix;
  uint64_t num_objs;

  po::options_description gen_opts("General options");
  gen_opts.add_options()
    ("help,h", "show help message")
    ("pool", po::value<std::string>(&pool)->required(), "pool")
    ("num-objs", po::value<uint64_t>(&num_objs)->required(), "number of objects to merge (assumes 'obj.' prefix)")
 ;

  po::options_description all_opts("Allowed options");
  all_opts.add(gen_opts);

  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, all_opts), vm);

  if (vm.count("help")) {
    std::cout << all_opts << std::endl;
    return 1;
  }

  po::notify(vm);

  // connect to rados
  librados::Rados cluster;
  cluster.init(NULL);
  cluster.conf_read_file(NULL);
  int ret = cluster.connect();
  checkret(ret, 0);

  // open pool
  librados::IoCtx ioctx;
  ret = cluster.ioctx_create(pool.c_str(), ioctx);
  checkret(ret, 0);

  int group_size = 10 ;
  int obj_count  = 0 ;
  int group_count  = 0 ;
  std::cout << num_objs << std::endl ;
  std::cout << group_size << std::endl ;
  std::cout << std::to_string(num_objs/group_size) << std::endl ;
  for( unsigned int i=0; i < (num_objs/group_size); i++ ) {
    for( int j=0; j < group_size; j++ ) {
      std::string target_objname = "obj.mergetarget."+std::to_string(group_count) ;
      std::string src_objname = "obj."+std::to_string(obj_count) ;
      //std::cout << target_objname << std::endl ;
      //std::cout << src_objname << std::endl ;
      librados::ObjectWriteOperation op;
      op.copy_from2(src_objname, ioctx, 0, librados::OP_FADVISE_COPYFROMAPPEND);
      ret = ioctx.operate(target_objname, &op);
      checkret(ret, 0);
      obj_count++ ;
    }
    group_count++ ;
  }

  ioctx.close();
  return 0;
}
//op.copy_from(src_objname, ioctx, 0);
