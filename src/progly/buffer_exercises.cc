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
#include "re2/re2.h"

// ceph includes
#include "include/rados/librados.hpp"


// relevant reference:
// src/include/buffer.h

int main(int argc, char **argv)
{
  std::cout << "start buffer exercises" << std::endl ;

  // the basic buffer list.
  // store anything! it's a bag of bytes.
  // observe weird characters at the end when printing the c_str.
  librados::bufferlist bl1 ;
  bl1.append("ABCK", 4);

  std::cout << bl1 << std::endl ;
  std::cout << bl1.c_str() << std::endl ;

  ceph::bufferptr ptr1 ;
  librados::bufferlist::iterator i( &bl1 ) ;
  i.copy_shallow( 2, ptr1 ) ;

  std::cout << &i << std::endl ;

  {
    // given bufferlist and index in bufferlist
    bufferlist::iterator i2( &bl1, 1 )  ;
    const buffer::ptr ptr = i2.get_current_ptr();
    std::cout << ptr[0] << std::endl ;  //B
    std::cout << ptr.offset() << std::endl ; //1
    std::cout << ptr.length() << std::endl ; //2
  }

  {
    // equiv bufferptr p1 = new buffer("one",3);
    // bufferptr p1 = buffer::copy("one",4);
    // buffer::ptr;
    bufferptr a("one", 3);
    bufferptr b("two", 3);
    bufferptr c("three", 5);
    bufferlist bl;
    bl.append(a);
    bl.append(b);
    bl.append(c);
    const char* ptr;
    bufferlist::iterator p = bl.begin();

    // get a pointer to the current iterator position, return the
    // number of bytes we can read from that position (up to want),
    // and advance the iterator by that amount.
    // size_t get_ptr_and_advance(size_t want, const char **p);
    // observe 3+3+5=11
    std::cout << "p.get_ptr_and_advance( 11, &ptr ) = " << p.get_ptr_and_advance( 11, &ptr ) << std::endl ; //3u

    std::cout << "ptr:" << std::endl ;
    std::cout << ptr[0] << std::endl ;
    std::cout << ptr[1] << std::endl ;
    std::cout << ptr[2] << std::endl ;
    std::cout << bl << std::endl ;
    int arr_size = (sizeof(ptr)/sizeof(*ptr)) ;
    for (int i = 0; i < arr_size; ++i )
    {
      std::cout << ptr[i] << std::endl ;
    }

    // get number of bytes remaining from iterator position to the end of the buffer::list 
    // unsigned get_remaining() const { return bl->length() - off; }
    std::cout << "p.get_remaining() = " << p.get_remaining() << std::endl ; //8

    // get current iterator offset in buffer::list
    // unsigned get_off() const { return off; }
    std::cout << "p.get_off() = " << p.get_off() << std::endl ; //3

    // int memcmp( const void* lhs, const void* rhs, std::size_t count );
    // lhs, rhs => pointers to the memory buffers to compare
    // count    => number of bytes to examine
    std::cout << memcmp(ptr, "one", 3) << std::endl ; //0
  }

  {
    bufferlist bl ;
    bufferlist::iterator i( &bl ) ;
    try
    { 
      ++i ; 
    } 
    catch ( const ceph::buffer::end_of_buffer& e )
    {
      std::cout << e.what() << std::endl ;
    }
    //EXPECT_THROW(++i, buffer::end_of_buffer) ;
  }

  {
    std::cout << "buffer_exercises.cc" << std::endl ;

    //bufferptr ptr ;
    //i.copy_shallow( 1, ptr);  // copy nothing
    //std::cout << "ptr.length() = " << ptr.length() << std::endl ; 
    //std::cout << "ptr[0] = " << ptr[0] << std::endl ; 
    //std::cout << "ptr[1] = " << ptr[1] << std::endl ;

    // Copy from iter into target bl
    bufferlist bl;
    bl.append("ABCD",4);
    bufferlist copy ;
    bufferlist::iterator i( &bl );  // you can also declare iterator at a pos:  i(&bl, 2);
    bufferlist::iterator i2( &copy );  // you can also declare iterator at a pos:  i(&bl, 2);
    bufferptr ptr ;
    i.copy_deep( 1, ptr);  // copy nothing
    ptr.set_length( 1 ) ;
    std::cout << "ptr.length() = " << ptr.length() << std::endl ; 
    std::cout << "ptr[0] = " << ptr[0] << std::endl ; 
    //std::cout << "ptr[1] = " << ptr[1] << std::endl ;
    bufferlist target ;
    target.append( ptr ) ;
    std::cout << target.c_str() << std::endl ;
    std::cout << target.length() << std::endl ;

    //i.copy( 1, copy ) ;  // copy nothing
    //std::cout << "copy.length() = " << copy.length() << std::endl ; 
    //std::cout << "i2.get_remaining() = " << i2.get_remaining() << std::endl ; 
    //std::cout << "copy.c_str() = " << copy.c_str() << std::endl ; 

    //std::cout << "*i" << *i << std::endl ; 
    //std::cout << "copy.length()" << copy.length() << std::endl ; 
    //i.copy( 1, copy);  // copy="A"
    //std::cout << "1,copy:" << std::endl ; 
    //std::cout << copy.c_str() << std::endl ; //huh? ABCD?
    //std::cout << "*i" << *i << std::endl ; 
    //std::cout << "copy.length()" << copy.length() << std::endl ; 
    //i.seek(0); 
    //i.copy(4, copy);  // copy=AABCD
    //std::cout << "4,copy:" << std::endl ; 
    //std::cout << copy.c_str() << std::endl ; //huh? ABCD?

    //i.seek(2); 
    //i.copy(1, copy) ;  // copy=AABCDC

    //i.seek(3); 
    //i.copy(1, copy) ;  // copy=AABCDCD

    //std::cout << bl.c_str() << std::endl ;
    //std::cout << copy.c_str() << std::endl ;
  }


  return 0 ;
}
