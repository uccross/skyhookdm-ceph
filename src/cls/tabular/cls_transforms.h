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

struct transform_args {

  std::string project_cols;

  transform_args() {}
  transform_args(std::string _project_cols) :
    project_cols(_project_cols) { }

  // serialize the fields into bufferlist to be sent over the wire
  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(project_cols, bl);
    ENCODE_FINISH(bl);
  }

  // deserialize the fields from the bufferlist into this struct
  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(project_cols, bl);
    DECODE_FINISH(bl);
  }

  std::string toString() {
    std::string s;
    s.append("transform_args:");
    s.append(" .project_cols =" + project_cols);
    return s;
  }
};
WRITE_CLASS_ENCODER(transform_args)

#endif
