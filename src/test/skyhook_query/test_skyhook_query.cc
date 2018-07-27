// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <iostream>
#include <thread>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>

#include "include/rados/librados.hpp"
#include "include/encoding.h"
#include "test/librados/test.h"
#include "gtest/gtest.h"

using namespace librados;

unsigned default_op_size = 1 << 22;
unsigned default_objsize = 14100000;
unsigned default_nobject = 10;
static const size_t extended_price_field_offset = 24;

class SkyhookQuery : public ::testing::Test {
  protected:
    static void SetUpTestCase() {
      std::cout << "Inside SetUpTestCase()" << std::endl;
      pool_name = get_temp_pool_name();
      ASSERT_EQ("", create_one_pool_pp(pool_name, rados));
      ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));

      std::string dirname = "/data/";
      for (int i = 0; i < (int) default_nobject; i++) {
        std::string oid = "obj000000" + std::to_string(i) + ".bin";
    
        // from rados tool
        std::cout << "  ... uploading " << (dirname + oid).c_str() << std::endl;
        int fd = open((dirname + oid).c_str(), O_RDONLY);
        ASSERT_GE(fd, 0);
    
        // how much should we read?
        int count = default_op_size;
        int offset = 0;
        while (count != 0) { 
          bufferlist indata;
          count = indata.read_fd(fd, default_op_size);
          ASSERT_GE(count, 0);
    
          // write the data into rados
          int ret = 0;
          if (offset == 0)
            ret = ioctx.write_full(oid, indata);
          else
            ret = ioctx.write(oid, indata, count, offset);
          ASSERT_GE(ret, 0);
    
          offset += count;
        }
        close(fd);
      }
 
      unsigned int nobjects = 0;
      ObjectCursor c = ioctx.object_list_begin();
      ObjectCursor end = ioctx.object_list_end();
      std::cout << "  ... checking size and number of objects" << std::endl;
      while(!ioctx.object_list_is_end(c)) {
        std::vector<ObjectItem> result;
        int n = ioctx.object_list(c, end, default_nobject, {}, &result, &c);
        nobjects += n;

        for (int i = 0; i < n; i++) {
          auto oid = result[i].oid;
          uint64_t psize;
          time_t pmtime;
          int r = ioctx.stat(oid, &psize, &pmtime);
          ASSERT_GE(r, 0);
          ASSERT_EQ(default_objsize, psize);
        }
      }
      ASSERT_EQ(default_nobject, nobjects);
    }

    static void TearDownTestCase() {
      ioctx.close();
      ASSERT_EQ(0, destroy_one_pool_pp(pool_name, rados));
    }

    static librados::Rados rados;
    static librados::IoCtx ioctx;
    static std::string pool_name;
};

librados::Rados SkyhookQuery::rados;
librados::IoCtx SkyhookQuery::ioctx;
std::string SkyhookQuery::pool_name;
static std::string string_ncopy(const char* buffer, std::size_t buffer_size) {
  const char* copyupto = std::find(buffer, buffer + buffer_size, 0);
  return std::string(buffer, copyupto);
}

void print_row(const char *row) {
  const size_t order_key_field_offset = 0;
  size_t line_number_field_offset = 12;
  const size_t quantity_field_offset = 16;
  const size_t extended_price_field_offset = 24;
  const size_t discount_field_offset = 32;
  const size_t shipdate_field_offset = 50;
  const size_t comment_field_offset = 97;
  const size_t comment_field_length = 44;

  const double extended_price = *((const double *)(row + extended_price_field_offset));
  const int order_key = *((const int *)(row + order_key_field_offset));
  const int line_number = *((const int *)(row + line_number_field_offset));
  const int ship_date = *((const int *)(row + shipdate_field_offset));
  const double discount = *((const double *)(row + discount_field_offset));
  const double quantity = *((const double *)(row + quantity_field_offset));
  const std::string comment = string_ncopy(row + comment_field_offset,
      comment_field_length);

  std::cout << extended_price <<
    "|" << order_key <<
    "|" << line_number <<
    "|" << ship_date <<
    "|" << discount <<
    "|" << quantity <<
    "|" << comment <<
    std::endl;
}

// returns all rows to clients
TEST_F(SkyhookQuery, NoCLS) {
  std::string query = "b";
  double extended_price = 91400.0;

  unsigned result_count = 0;
  unsigned rows_returned = 0;
  for (int i = 0; i < (int) default_nobject; i++) {
    std::string oid = "obj000000" + std::to_string(i) + ".bin";
    bufferlist data;
    librados::AioCompletion *c = librados::Rados::aio_create_completion();
    int ret = ioctx.aio_read(oid, c, &data, 0, 0);
    ASSERT_GE(ret, 0);

    c->wait_for_complete();
    ret = c->get_return_value();
    ASSERT_GE(ret, 0);

    // apply the query
    size_t row_size = 141;
    const char *rows = data.c_str();
    const size_t num_rows = data.length() / row_size;
    rows_returned += num_rows;
    for (size_t rid = 0; rid < num_rows; rid++) {
      const char *row = rows + rid * row_size;  // index in
      const char *vptr = row + extended_price_field_offset;
      const double val = *(const double*)vptr;

      if (val > extended_price) {
        //print_row(row);
        result_count++;
      }
    }
  }
  ASSERT_EQ(10172, (int) result_count);
  ASSERT_EQ(1000000, (int) rows_returned);
}

