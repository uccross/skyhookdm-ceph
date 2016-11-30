#include <errno.h>
#include <string>
#include <sstream>
#include "include/types.h"
#include "objclass/objclass.h"
#include "cls_tabular.h"

CLS_VER(1,0)
CLS_NAME(tabular)

cls_handle_t h_class;
cls_method_handle_t h_scan;

struct tab_index {
  uint64_t min;
  uint64_t max;

  tab_index() {}

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(min, bl);
    ::encode(max, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(min, bl);
    ::decode(max, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(tab_index)

static int scan(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  scan_op op;

  try {
    bufferlist::iterator it = in->begin();
    ::decode(op, it);
  } catch (const buffer::error &err) {
    CLS_ERR("ERROR: decoding scan op");
    return -EINVAL;
  }

  struct tab_index idx;
  bool has_index = false;

  if (op.use_index) {
    // read index header
    bufferlist hdrbl;
    int ret = cls_cxx_map_read_header(hctx, &hdrbl);
    if (ret < 0) {
      CLS_ERR("ERROR: reading map hdr %d", ret);
      return ret;
    }

    if (hdrbl.length() > 0) {
      try {
        bufferlist::iterator it = hdrbl.begin();
        ::decode(idx, it);
        has_index = true;
      } catch (const buffer::error &err) {
        CLS_ERR("ERROR: decoding index");
        return -EIO;
      }

      if (op.max_val < idx.min)
        return 0;
    }
  }

  bufferlist bl;
  int ret = cls_cxx_read(hctx, 0, 0, &bl);
  if (ret < 0) {
    CLS_ERR("ERROR: reading obj %d", ret);
    return ret;
  }

  const uint64_t *rows = (uint64_t*)bl.c_str();
  const uint64_t row_count = bl.length() / sizeof(uint64_t);

  // build the index?
  if (op.use_index && !has_index) {
    if (row_count > 0) {
      idx.min = idx.max = rows[0];
      for (unsigned i = 0; i < row_count; i++) {
        const uint64_t row_val = rows[i];
        idx.min = std::min(row_val, idx.min);
        idx.max = std::max(row_val, idx.max);
        if (row_val < op.max_val)
          out->append((char*)&row_val, sizeof(row_val));
      }

      bufferlist hdrbl;
      ::encode(idx, hdrbl);
      ret = cls_cxx_map_write_header(hctx, &hdrbl);
      if (ret < 0) {
        CLS_ERR("ERROR: writing obj hdr %d", ret);
        return ret;
      }
    }
  } else {
    for (unsigned i = 0; i < row_count; i++) {
      const uint64_t row_val = rows[i];
      if (row_val < op.max_val)
        out->append((char*)&row_val, sizeof(row_val));
    }
  }

  return 0;
}

void __cls_init()
{
  CLS_LOG(20, "Loaded tabular class!");

  cls_register("tabular", &h_class);

  cls_register_cxx_method(h_class, "scan",
      CLS_METHOD_RD | CLS_METHOD_WR, scan, &h_scan);
}
