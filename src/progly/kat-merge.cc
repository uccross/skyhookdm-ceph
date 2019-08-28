/*
* Copyright (C) 2018 The Regents of the University of California
* All Rights Reserved
*
* This library can redistribute it and/or modify under the terms
* of the GNU Lesser General Public License Version 2.1 as published
* by the Free Software Foundation.
*
*/

#include "include/rados/librados.hpp" // for librados
#include <boost/algorithm/string.hpp> // for boost::trim
#include <fstream>
#include <boost/program_options.hpp>
#include "cls/tabular/cls_tabular_utils.h"

namespace po = boost::program_options;

int main(int argc, char **argv)
{
    std::string pool;
    unsigned num_objs;
    std::string obj_prefix ;
    bool use_cls ;
  
    po::options_description gen_opts("General options");
    gen_opts.add_options()
      ("help,h", "show help message")
      ("pool", po::value<std::string>(&pool)->required(), "pool")
      ("num-objs", po::value<unsigned>(&num_objs)->required(), "num objects")
      ("objs-prefix", po::value<std::string>(&obj_prefix)->required(), "prefix for regularized object names eg 'obj.'")
      ("use-cls", po::value<bool>(&use_cls)->required(), "use-cls")
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
  
    assert(num_objs > 0);

    std::vector< std::string > obj_name_list ;
    for(unsigned int i = 0; i < num_objs; i++)
        obj_name_list.push_back(obj_prefix+std::to_string(i)) ;
    std::string a = "obj."+std::to_string(obj_name_list.size()-1)+"00000" ;

    // --------------------------------- //
    // connect to rados
    librados::Rados cluster;
    cluster.init(NULL);
    cluster.conf_read_file(NULL);
    int ret = cluster.connect();
    checkret(ret, 0);
    // open pool
    librados::IoCtx ioctx ;
    ret = cluster.ioctx_create(pool.c_str(), ioctx);
    checkret(ret, 0);
    // --------------------------------- //

    if(use_cls) {  
        // do the merge
        librados::bufferlist merged_bl ;
        for(unsigned int i=0; i<num_objs; i++) {
            std::string oid = obj_name_list[i];

            // read an object bl
            librados::bufferlist obj_bl ;
            int num_bytes_read = ioctx.read(oid.c_str(), obj_bl, (size_t)0, (uint64_t)0);
            std::cout << "num_bytes_read : " << num_bytes_read << std::endl;

            // copy data from object bl into the merged bl
            bufferlist::iterator obj_it(&obj_bl);    // you can also declare iterator at a pos:  i(&bl, 2);
            obj_it.copy(num_bytes_read, merged_bl);  // copy all the bytes from the read bl into the merged bl
        }
  
        // write the merged bl
        const char* merged_obj_name = a.c_str() ;
        bufferlist::iterator p = merged_bl.begin();
        size_t num_bytes_written = p.get_remaining() ;
        std::cout << "num bytes written : " << num_bytes_written << std::endl ;
        ret = ioctx.write( merged_obj_name, merged_bl, num_bytes_written, 0 ) ;
        checkret(ret,0);
    }
    else {
        std::cout << "blah" << std::endl ;

        std::string merged_obj_name = a ;

        // create merge_args
        //merge_args args(pool, 
        //                num_objs, 
        //                obj_prefix,
        //                ioctx);
        std::cout << "blah" << std::endl ;
        //ceph::bufferlist inbl, outbl;
        //::encode(args, inbl);
        //std::cout << "blah" << std::endl ;
        //ret = ioctx.exec(merged_obj_name, "tabular", "merge_op",
        //                 inbl, outbl);
        //std::cout << "blah" << std::endl ;
        //checkret(ret, 0);
    }

    ioctx.close();
    return 0;
}
