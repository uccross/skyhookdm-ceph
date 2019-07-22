
// bin/fbreader --oid atable_transposed --pool tpchflatbuf --debug yes

#include "include/types.h"
#include <string>
#include <sstream>
#include <type_traits>
#include <errno.h>
#include <time.h>
#include <boost/algorithm/string/classification.hpp> // for boost::is_any_of
#include <boost/algorithm/string/split.hpp> // for boost::split
#include <boost/algorithm/string.hpp> // for boost::trim
#include <boost/lexical_cast.hpp>
#include <boost/program_options.hpp>

#include "re2/re2.h"
#include "objclass/objclass.h"
#include <iostream>
#include <algorithm>
#include <atomic>
#include <thread>
#include <condition_variable>
#include "re2/re2.h"
#include "include/rados/librados.hpp"

#include "cls_tabular_utils.h"

namespace po = boost::program_options ;

void do_read( bool, std::string, std::string ) ;

int main( int argc, char *argv[] ) {

  bool debug ;
  std::string oid  ;
  std::string pool ;

  po::options_description gen_opts("General options");
  gen_opts.add_options()
    ("help,h", "show help message")
    ("debug", po::value<bool>(&debug)->required(), "debug")
    ("oid", po::value<std::string>(&oid)->required(), "oid")
    ("pool", po::value<std::string>(&pool)->required(), "pool") ;

  po::options_description all_opts( "Allowed options" ) ;
  all_opts.add( gen_opts ) ;
  po::variables_map vm ;
  po::store( po::parse_command_line( argc, argv, all_opts ), vm ) ;
  if( vm.count( "help" ) ) {
    std::cout << all_opts << std::endl ;
    return 1;
  }
  po::notify( vm ) ;

  do_read( debug, oid, pool ) ;

  return 0 ;
}

// ========== //
//   DO READ  //
// ========== //
void do_read( bool debug, 
              std::string oid,
              std::string pool ) {

  if( debug ) {
    std::cout << "in do_read..." << std::endl ;
    std::cout << "oid         : " << oid         << std::endl ;
    std::cout << "pool        : " << pool        << std::endl ;
  }

  // connect to rados
  librados::Rados cluster;
  cluster.init(NULL);
  cluster.conf_read_file(NULL);
  int ret = cluster.connect();
  checkret( ret, 0 ) ;

  // open pool
  librados::IoCtx ioctx ;
  ret = cluster.ioctx_create( pool.c_str(), ioctx ) ;

  // read bl_seq
  librados::bufferlist wrapped_bl_seq ;
  int num_bytes_read = ioctx.read( oid.c_str(), wrapped_bl_seq, (size_t)0, (uint64_t)0 ) ;

  if( debug )
    std::cout << "num_bytes_read : " << num_bytes_read << std::endl ; 

  ceph::bufferlist::iterator it_wrapped = wrapped_bl_seq.begin() ;

  // ================================================================================ //
  // display data

  while( it_wrapped.get_remaining() > 0 ) {

    if( debug )
      std::cout << "it_wrapped.get_remaining() = " << it_wrapped.get_remaining() << std::endl ;

    // grab the Root
    ceph::bufferlist bl ;
    ::decode( bl, it_wrapped ) ; // this decrements get_remaining by moving iterator
    const char* dataptr = bl.c_str() ;
    size_t datasz       = bl.length() ;
    bool print_header   = true ;
    bool print_verbose  = false ;
    if( debug )
      print_verbose  = true ;
    long long int max_to_print = 0 ;

    Tables::printFlatbufFBUAsCSV(
      dataptr,
      datasz,
      print_header,
      print_verbose,
      max_to_print ) ;

    if( debug )
      std::cout << "loop while" << std::endl ;

  } // while

  ioctx.close() ;

} // do_read
