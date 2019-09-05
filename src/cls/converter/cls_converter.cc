// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * This is a simple example RADOS class, designed to be usable as a
 * template for implementing new methods.
 *
 * Our goal here is to illustrate the interface between the OSD and
 * the class and demonstrate what kinds of things a class can do.
 *
 * Note that any *real* class will probably have a much more
 * sophisticated protocol dealing with the in and out data buffers.
 * For an example of the model that we've settled on for handling that
 * in a clean way, please refer to cls_lock or cls_version for
 * relatively simple examples of how the parameter encoding can be
 * encoded in a way that allows for forward and backward compatibility
 * between client vs class revisions.
 */

/*
 * A quick note about bufferlists:
 *
 * The bufferlist class allows memory buffers to be concatenated,
 * truncated, spliced, "copied," encoded/embedded, and decoded.  For
 * most operations no actual data is ever copied, making bufferlists
 * very convenient for efficiently passing data around.
 *
 * bufferlist is actually a typedef of buffer::list, and is defined in
 * include/buffer.h (and implemented in common/buffer.cc).
 */

#include "my_table_generated.h"
#include "objclass/objclass.h"

using namespace ceph;
using namespace conversion;

CLS_VER(1,0)
CLS_NAME(converter)

struct col_entry {
  flatbuffers::FlatBufferBuilder* builder;
  MyTableBuilder* my_table_builder;
  uint64_t* vals;
};

static int convert(cls_method_context_t hctx, bufferlist *in, bufferlist *out) {

  unsigned char cid = *(reinterpret_cast<unsigned char*>(in->c_str()));

  // assuming that each omap entry holds the size of a row
  map<string, bufferlist> omap;
  bool more;
  int r = cls_cxx_map_get_all_vals(hctx, &omap, &more);
  if (r < 0) {
    return r;
  }

  map<int, int> offsets;
  for (map<string, bufferlist>::iterator it = omap.begin(); it != omap.end(); it++) {
    offsets[atoi(it->first.c_str())] = atoi(it->second.c_str());
  }

  int num_rows = offsets.size();

  col_entry c;
  c.builder = new flatbuffers::FlatBufferBuilder(1024*1024);
  uint8_t* buf;
  auto col_fb = c.builder->CreateUninitializedVector(num_rows, sizeof(uint64_t), &buf);
  c.my_table_builder = new MyTableBuilder(*c.builder);
  c.my_table_builder->add_id(cid);
  c.my_table_builder->add_data(col_fb);
  c.vals = reinterpret_cast<uint64_t*>(buf);
  auto v = c.my_table_builder->Finish();
  c.builder->Finish(v);

  // read the entire data
  bufferlist bls;
  r = cls_cxx_read(hctx, 0, 0, &bls);
  if (r < 0) {
    CLS_ERR("%s: error reading data %d", __FUNCTION__, r);
    return r;
  }
  void *p = bls.c_str();
  const char *rec = (const char*)p;

  // extract the field corresponding to the specified column id from all the rows
  uint64_t first_rid;
  uint64_t rid = 0;
  for (map<int, int>::iterator it = offsets.begin(); it != offsets.end(); it++) {
    int row_size = it->second;
    auto my_table = GetMyTable(rec);
    if (rid == 0) {
      first_rid = my_table->id();
    }
    auto d = my_table->data();
    c.vals[rid] = d->Get(cid);
    rec += row_size;
    rid++;
  }

  // put converted data into the buffer
  map<string, bufferlist> converted_omap;
  flatbuffers::FlatBufferBuilder *builder = c.builder;
  out->append(reinterpret_cast<const char*>(builder->GetBufferPointer()), static_cast<unsigned int>(builder->GetSize()));
  std::stringstream ss1;
  ss1 << std::setw(10) << std::setfill('0') << first_rid << std::endl;
  std::stringstream ss2;
  ss2 << builder->GetSize() << std::endl;
  bufferlist bl;
  bl.append(ss2.str().c_str());
  converted_omap[ss1.str().c_str()] = bl;

  int data_length = out->length();

  // put converted omap in the buffer
  uint32_t omap_keys = converted_omap.size();
  encode(omap_keys, *out);
  for (map<string, bufferlist>::const_iterator it = converted_omap.begin(); it != converted_omap.end(); ++it) {
    encode(it->first, *out);
    encode(it->second, *out);
  }

  // return the data length in the buffer so that the caller can separate data and omap
  return data_length;
}

/**
 * initialize class
 *
 * We do two things here: we register the new class, and then register
 * all of the class's methods.
 */
CLS_INIT(converter)
{
  // this log message, at level 0, will always appear in the ceph-osd
  // log file.
  CLS_LOG(0, "loading cls_converter");

  cls_handle_t h_class;
  cls_method_handle_t h_convert;

  cls_register("converter", &h_class);

  // There are two flags we specify for methods:
  //
  //    RD : whether this method (may) read prior object state
  //    WR : whether this method (may) write or update the object
  //
  // A method can be RD, WR, neither, or both.  If a method does
  // neither, the data it returns to the caller is a function of the
  // request and not the object contents.

  cls_register_cxx_method(h_class, "convert",
			  CLS_METHOD_RD,
			  convert, &h_convert);
}
