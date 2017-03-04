#include <errno.h>
#include <string>
#include <sstream>
#include "re2/re2.h"
#include "include/types.h"
#include "objclass/objclass.h"
#include "cls_tabular.h"

CLS_VER(1,0)
CLS_NAME(tabular)

cls_handle_t h_class;
cls_method_handle_t h_scan;
cls_method_handle_t h_read;
cls_method_handle_t h_add;
cls_method_handle_t h_regex_scan;

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

static int add(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  const uint64_t *rows = (uint64_t*)in->c_str();
  const uint64_t row_count = in->length() / sizeof(uint64_t);

  if (row_count == 0) {
    CLS_ERR("NO ROWS");
    return -EINVAL;
  }

  struct tab_index idx;

  idx.min = idx.max = rows[0];
  for (unsigned i = 0; i < row_count; i++) {
    const uint64_t row_val = rows[i];
    idx.min = std::min(row_val, idx.min);
    idx.max = std::max(row_val, idx.max);
  }

#if 0
  bufferlist hdrbl;
  ::encode(idx, hdrbl);
  int ret = cls_cxx_map_write_header(hctx, &hdrbl);
  if (ret < 0) {
    CLS_ERR("ERROR: writing obj hdr %d", ret);
    return ret;
  }
#endif

  bufferlist attr_bl;
  ::encode(idx.min, attr_bl);
  int ret = cls_cxx_setxattr(hctx, "min", &attr_bl);
  if (ret < 0) {
    CLS_ERR("ERROR: writing min xattr %d", ret);
    return ret;
  }

  ret = cls_cxx_write_full(hctx, in);
  if (ret < 0) {
    CLS_ERR("ERROR: writing obj full %d", ret);
    return ret;
  }

  return 0;
}

static int read(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  bufferlist bl;
  int ret = cls_cxx_read(hctx, 0, 0, &bl);
  if (ret < 0) {
    CLS_ERR("ERROR: reading obj %d", ret);
    return ret;
  }

  ::encode(bl, *out);

  return 0;
}

static int regex_scan(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  regex_scan_op op;

  try {
    bufferlist::iterator it = in->begin();
    ::decode(op, it);
  } catch (const buffer::error &err) {
    CLS_ERR("ERROR: decoding regex scan op");
    return -EINVAL;
  }

  bufferlist bl;
  int ret = cls_cxx_read(hctx, 0, 0, &bl);
  if (ret < 0) {
    CLS_ERR("ERROR: reading obj %d", ret);
    return ret;
  }

  const char *rows = bl.c_str();
  size_t num_rows = bl.length() / op.row_size;

  bufferlist rows_bl;
  rows_bl.reserve(bl.length());
  for (size_t r = 0; r < num_rows; r++) {
    const char *row = rows + r * op.row_size;
    const char *cptr = row + op.field_offset;
    const std::string comment(cptr, op.field_length);
    if (op.regex == "" || RE2::PartialMatch(comment, op.regex))
      rows_bl.append(row, op.row_size);
  }

  ::encode(rows_bl, *out);

  return 0;
}

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

  bool index_skipped = false;
  uint64_t buffer[8192];
  bufferlist rows_bl;

  if (op.use_index) {
#if 0
    struct tab_index idx;

    bufferlist hdrbl;
    int ret = cls_cxx_map_read_header(hctx, &hdrbl);
    if (ret < 0) {
      CLS_ERR("ERROR: reading map hdr %d", ret);
      return ret;
    }

    if (hdrbl.length() == 0) {
      CLS_ERR("ERROR: no header");
      return -EINVAL;
    }

    try {
      bufferlist::iterator it = hdrbl.begin();
      ::decode(idx, it);
    } catch (const buffer::error &err) {
      CLS_ERR("ERROR: decoding index");
      return -EIO;
    }
#endif

    bufferlist attr_bl;
    int ret = cls_cxx_getxattr(hctx, "min", &attr_bl);
    if (ret < 0) {
      CLS_ERR("ERROR: reading min attr: %d", ret);
      return ret;
    }

    if (attr_bl.length() == 0) {
      CLS_ERR("ERROR: no min attr");
      return -EINVAL;
    }

    uint64_t min_val;
    try {
      bufferlist::iterator it = attr_bl.begin();
      ::decode(min_val, it);
    } catch (const buffer::error&) {
      CLS_ERR("ERROR: decoding min attr");
      return -EIO;
    }

    if (op.max_val < min_val) {
      index_skipped = true;
      ::encode(index_skipped, *out);
      ::encode(rows_bl, *out);
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

  /*
   * TODO:
   *  - can we use loop unrolling, vector ops, or other techniques to speed
   *  this up?
   */
  if (op.naive) {
    if (op.preallocate) {
      rows_bl.reserve(bl.length());
    }
    for (unsigned i = 0; i < row_count; i++) {
      const uint64_t row_val = rows[i];
      if (row_val < op.max_val) {
        rows_bl.append((char*)&row_val, sizeof(row_val));
      }
    }
  } else {
    rows_bl.reserve(bl.length());
    size_t curr_entry = 0;
    for (unsigned i = 0; i < row_count; i++) {
      const uint64_t row_val = rows[i];
      if (row_val < op.max_val) {
        buffer[curr_entry] = row_val;
        if (++curr_entry == 8192) {
          rows_bl.append((char*)buffer,
              sizeof(uint64_t) * curr_entry);
          curr_entry = 0;
        }
      }
    }
    if (curr_entry > 0) {
      rows_bl.append((char*)buffer,
          sizeof(uint64_t) * curr_entry);
    }
  }

  ::encode(index_skipped, *out);
  ::encode(rows_bl, *out);

  return 0;
}

void __cls_init()
{
  CLS_LOG(20, "Loaded tabular class!");

  cls_register("tabular", &h_class);

  cls_register_cxx_method(h_class, "read",
      CLS_METHOD_RD | CLS_METHOD_WR, read, &h_read);

  cls_register_cxx_method(h_class, "scan",
      CLS_METHOD_RD | CLS_METHOD_WR, scan, &h_scan);

  cls_register_cxx_method(h_class, "add",
      CLS_METHOD_RD | CLS_METHOD_WR, add, &h_add);

  cls_register_cxx_method(h_class, "regex_scan",
      CLS_METHOD_RD, regex_scan, &h_regex_scan);
}
