/*
* Copyright (C) 2018 The Regents of the University of California
* All Rights Reserved
*
* This library can redistribute it and/or modify under the terms
* of the GNU Lesser General Public License Version 2.1 as published
* by the Free Software Foundation.
*
*/


#include <errno.h>
#include <string>
#include <sstream>
#include <boost/lexical_cast.hpp>
#include <time.h>
#include "re2/re2.h"
#include "include/types.h"
#include "objclass/objclass.h"
#include "cls_transforms_utils.h"
#include "cls_transforms.h"

CLS_VER(1,0)
CLS_NAME(transforms)

cls_handle_t h_class;
cls_method_handle_t h_transform;

/*
 * Function: transform
 * Description: Method to convert database format.
 * @param[in] hctx    : CLS method context
 * @param[out] in     : input bufferlist
 * @param[out] out    : output bufferlist
 * Return Value: error code
*/

static
int transform(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
    return 0;
}

void __cls_init()
{
  CLS_LOG(20, "Loaded transform class!");

  cls_register("transform", &h_class);

  cls_register_cxx_method(h_class, "transform",
      CLS_METHOD_RD | CLS_METHOD_WR, transform, &h_transform);
}
