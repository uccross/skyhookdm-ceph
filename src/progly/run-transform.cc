/*
* Copyright (C) 2018 The Regents of the University of California
* All Rights Reserved
*
* This library can redistribute it and/or modify under the terms
* of the GNU Lesser General Public License Version 2.1 as published
* by the Free Software Foundation.
*
*/

// std libs
#include <fstream>
#include <boost/program_options.hpp>
#include <iostream>
#include <algorithm>
#include <atomic>
#include <thread>
#include <condition_variable>
#include <chrono>
#include "re2/re2.h"

// ceph includes
#include "include/rados/librados.hpp"

// skyhook includes
#include "cls/transform/cls_transform.h"

namespace po = boost::program_options;

int main(int argc, char **argv)
{
  std::cout << "starting transforms..." << std::endl ;

  Transform::transform() ;

  std::cout << "...transformss done. phew!" << std::endl ;
  return 0 ;
}
