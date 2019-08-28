/*
* Copyright (C) 2018 The Regents of the University of California
* All Rights Reserved
*
* This library can redistribute it and/or modify under the terms
* of the GNU Lesser General Public License Version 2.1 as published
* by the Free Software Foundation.
*
*/


#ifndef CLS_TRANSFORMS_H
#define CLS_TRANSFORMS_H

#include "include/types.h"

//struct transform_args {
//
//  std::string table_name;
//  std::string data_schema;
//  int required_type;
//
//  transform_args() {}
//  transform_args(std::string tname, std::string dtscma, int req_type) :
//    table_name(tname), data_schema(dtscma), required_type(req_type) { }
//
//  // serialize the fields into bufferlist to be sent over the wire
//  void encode(bufferlist& bl) const {
//    ENCODE_START(1, 1, bl);
//    ::encode(table_name, bl);
//    ::encode(data_schema, bl);
//    ::encode(required_type, bl);
//    ENCODE_FINISH(bl);
//  }
//
//  // deserialize the fields from the bufferlist into this struct
//  void decode(bufferlist::iterator& bl) {
//    DECODE_START(1, bl);
//    ::decode(table_name, bl);
//    ::decode(data_schema, bl);
//    ::decode(required_type, bl);
//    DECODE_FINISH(bl);
//  }
//
//  std::string toString() {
//    std::string s;
//    s.append("transform_op:");
//    s.append(" .table_name=" + table_name);
//    s.append(" .data_schema=" + data_schema);
//    s.append(" .required_type=" + std::to_string(required_type));
//    return s;
//  }
//};
//WRITE_CLASS_ENCODER(transform_args)

#endif
