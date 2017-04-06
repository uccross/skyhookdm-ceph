#include <errno.h>
#include <string>
#include <sstream>
#include <boost/lexical_cast.hpp>
#include <time.h>
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
cls_method_handle_t h_query_op;
cls_method_handle_t h_build_index;
cls_method_handle_t h_test_par;

static inline uint64_t __getns(clockid_t clock)
{
  struct timespec ts;
  int ret = clock_gettime(clock, &ts);
  assert(ret == 0);
  return (((uint64_t)ts.tv_sec) * 1000000000ULL) + ts.tv_nsec;
}

static inline uint64_t getns()
{
  return __getns(CLOCK_MONOTONIC);
}

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

static std::string string_ncopy(const char* buffer, std::size_t buffer_size) {
  const char* copyupto = std::find(buffer, buffer + buffer_size, 0);
  return std::string(buffer, copyupto);
}

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

static inline std::string u64tostr(uint64_t value)
{
  std::stringstream ss;
  ss << std::setw(20) << std::setfill('0') << value;
  return ss.str();
}
 
/*
 * Convert string into numeric value.
 */
static inline int strtou64(const std::string value, uint64_t *out)
{
  uint64_t v;

  try {
    v = boost::lexical_cast<uint64_t>(value);
  } catch (boost::bad_lexical_cast &) {
    CLS_ERR("converting key into numeric value %s", value.c_str());
    return -EIO;
  }
 
  *out = v;
  return 0;
}

static int build_index(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  uint32_t batch_size;

  try {
    bufferlist::iterator it = in->begin();
    ::decode(batch_size, it);
  } catch (const buffer::error &err) {
    CLS_ERR("ERROR: decoding batch_size");
    return -EINVAL;
  }

  bufferlist bl;
  int ret = cls_cxx_read(hctx, 0, 0, &bl);
  if (ret < 0) {
    CLS_ERR("ERROR: reading obj %d", ret);
    return ret;
  }

  const size_t row_size = 141;
  const char *rows = bl.c_str();
  const size_t num_rows = bl.length() / row_size;

  const size_t order_key_field_offset = 0;
  const size_t line_number_field_offset = 12;


  std::map<string, bufferlist> index;

  for (size_t rid = 0; rid < num_rows; rid++) {
    const char *row = rows + rid * row_size;
    const char *o_vptr = row + order_key_field_offset;
    const int order_key_val = *(const int*)o_vptr;
    const char *l_vptr = row + line_number_field_offset;
    const int line_number_val = *(const int*)l_vptr;

    // key
    uint64_t key = ((uint64_t)order_key_val) << 32;
    key |= (uint32_t)line_number_val;
    const std::string strkey = u64tostr(key);

    // val
    bufferlist row_offset_bl;
    const size_t row_offset = rid * row_size;
    ::encode(row_offset, row_offset_bl);

    if (index.count(strkey) != 0)
      return -EINVAL;

    index[strkey] = row_offset_bl;

    if (index.size() > batch_size) {
      int ret = cls_cxx_map_set_vals(hctx, &index);
      if (ret < 0) {
        CLS_ERR("error setting index entries %d", ret);
        return ret;
      }
      index.clear();
    }
  }

  if (!index.empty()) {
    int ret = cls_cxx_map_set_vals(hctx, &index);
    if (ret < 0) {
      CLS_ERR("error setting index entries %d", ret);
      return ret;
    }
  }

  return 0;
}

static int test_par(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  uint64_t iters;
  bool readobj;

  try {
    bufferlist::iterator it = in->begin();
    ::decode(iters, it);
    ::decode(readobj, it);
  } catch (const buffer::error &err) {
    CLS_ERR("ERROR: decoding iters");
    return -EINVAL;
  }

  if (readobj) {
    bufferlist bl;
    int ret = cls_cxx_read(hctx, 0, 0, &bl);
    if (ret < 0) {
      CLS_ERR("ERROR: reading obj %d", ret);
      return ret;
    }
  }

  int sum = 0;
  iters = std::min(iters, (uint64_t)10000000000ULL);
  for (uint64_t count = 0; count < iters; count++)
    sum += (int)count;

  ::encode(sum, *out);

  return 0;
}

// busy loop work
volatile uint64_t __tabular_x;
static void add_extra_row_cost(uint64_t cost)
{
  for (uint64_t i = 0; i < cost; i++) {
    __tabular_x += i;
  }
}

static int query_op_op(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  query_op op;

  try {
    bufferlist::iterator it = in->begin();
    ::decode(op, it);
  } catch (const buffer::error &err) {
    CLS_ERR("ERROR: decoding query op");
    return -EINVAL;
  }

  bufferlist bl;

  uint64_t read_ns = 0;
  if (op.query != "d" || !op.use_index) {
    uint64_t start = getns();
    int ret = cls_cxx_read(hctx, 0, 0, &bl);
    if (ret < 0) {
      CLS_ERR("ERROR: reading obj %d", ret);
      return ret;
    }
    read_ns = getns() - start;
  }

  uint64_t eval_ns_start = getns();

  const size_t row_size = 141;
  const char *rows = bl.c_str();
  const size_t num_rows = bl.length() / row_size;

  bufferlist result_bl;
  result_bl.reserve(bl.length());

  const size_t order_key_field_offset = 0;
  const size_t line_number_field_offset = 12;
  const size_t quantity_field_offset = 16;
  const size_t extended_price_field_offset = 24;
  const size_t discount_field_offset = 32;
  const size_t shipdate_field_offset = 50;
  const size_t comment_field_offset = 97;
  const size_t comment_field_length = 44;

  if (op.query == "a") {
    size_t result_count = 0;
    for (size_t rid = 0; rid < num_rows; rid++) {
      const char *row = rows + rid * row_size;
      const char *vptr = row + extended_price_field_offset;
      const double val = *(const double*)vptr;
      if (val > op.extended_price) {
        result_count++;
        // when a predicate passes, add some extra work
        add_extra_row_cost(op.extra_row_cost);
      }
    }
    ::encode(result_count, result_bl);
  } else if (op.query == "b") {
    if (op.projection) {
      for (size_t rid = 0; rid < num_rows; rid++) {
        const char *row = rows + rid * row_size;
        const char *vptr = row + extended_price_field_offset;
        const double val = *(const double*)vptr;
        if (val > op.extended_price) {
          result_bl.append(row + order_key_field_offset, 4);
          result_bl.append(row + line_number_field_offset, 4);
          add_extra_row_cost(op.extra_row_cost);
        }
      }
    } else {
      for (size_t rid = 0; rid < num_rows; rid++) {
        const char *row = rows + rid * row_size;
        const char *vptr = row + extended_price_field_offset;
        const double val = *(const double*)vptr;
        if (val > op.extended_price) {
          result_bl.append(row, row_size);
          add_extra_row_cost(op.extra_row_cost);
        }
      }
    }
  } else if (op.query == "c") {
    if (op.projection) {
      for (size_t rid = 0; rid < num_rows; rid++) {
        const char *row = rows + rid * row_size;
        const char *vptr = row + extended_price_field_offset;
        const double val = *(const double*)vptr;
        if (val == op.extended_price) {
          result_bl.append(row + order_key_field_offset, 4);
          result_bl.append(row + line_number_field_offset, 4);
          add_extra_row_cost(op.extra_row_cost);
        }
      }
    } else {
      for (size_t rid = 0; rid < num_rows; rid++) {
        const char *row = rows + rid * row_size;
        const char *vptr = row + extended_price_field_offset;
        const double val = *(const double*)vptr;
        if (val == op.extended_price) {
          result_bl.append(row, row_size);
          add_extra_row_cost(op.extra_row_cost);
        }
      }
    }
  } else if (op.query == "d") {
    if (op.use_index) {
      uint64_t key = ((uint64_t)op.order_key) << 32;
      key |= (uint32_t)op.line_number;
      const std::string strkey = u64tostr(key);

      bufferlist row_offset_bl;
      int ret = cls_cxx_map_get_val(hctx, strkey, &row_offset_bl);
      if (ret < 0 && ret != -ENOENT) {
        CLS_ERR("cant read map val index %d", ret);
        return ret;
      }

      if (ret >= 0) {
        size_t row_offset;
        bufferlist::iterator it = row_offset_bl.begin();
        try {
          ::decode(row_offset, it);
        } catch (const buffer::error &err) {
          CLS_ERR("ERR cant decode index entry");
          return -EIO;
        }

        size_t size;
        int ret = cls_cxx_stat(hctx, &size, NULL);
        if (ret < 0) {
          CLS_ERR("ERR stat %d", ret);
          return ret;
        }

        if ((row_offset + row_size) > size) {
          return -EIO;
        }

        bufferlist bl;
        ret = cls_cxx_read(hctx, row_offset, row_size, &bl);
        if (ret < 0) {
          CLS_ERR("ERROR: reading obj %d", ret);
          return ret;
        }

        if (bl.length() != row_size) {
          CLS_ERR("unexpected read size");
          return -EIO;
        }

        const char *row = bl.c_str();

        if (op.projection) {
          result_bl.append(row + order_key_field_offset, 4);
          result_bl.append(row + line_number_field_offset, 4);
        } else {
          result_bl.append(row, row_size);
        }
      }

    } else {
      if (op.projection) {
        for (size_t rid = 0; rid < num_rows; rid++) {
          const char *row = rows + rid * row_size;
          const char *vptr = row + order_key_field_offset;
          const int order_key_val = *(const int*)vptr;
          if (order_key_val == op.order_key) {
            const char *vptr = row + line_number_field_offset;
            const int line_number_val = *(const int*)vptr;
            if (line_number_val == op.line_number) {
              result_bl.append(row + order_key_field_offset, 4);
              result_bl.append(row + line_number_field_offset, 4);
              add_extra_row_cost(op.extra_row_cost);
            }
          }
        }
      } else {
        for (size_t rid = 0; rid < num_rows; rid++) {
          const char *row = rows + rid * row_size;
          const char *vptr = row + order_key_field_offset;
          const int order_key_val = *(const int*)vptr;
          if (order_key_val == op.order_key) {
            const char *vptr = row + line_number_field_offset;
            const int line_number_val = *(const int*)vptr;
            if (line_number_val == op.line_number) {
              result_bl.append(row, row_size);
              add_extra_row_cost(op.extra_row_cost);
            }
          }
        }
      }
    }
  } else if (op.query == "e") {
    if (op.projection) {
      for (size_t rid = 0; rid < num_rows; rid++) {
        const char *row = rows + rid * row_size;

        const int shipdate_val = *((const int *)(row + shipdate_field_offset));
        if (shipdate_val >= op.ship_date_low && shipdate_val < op.ship_date_high) {
          const double discount_val = *((const double *)(row + discount_field_offset));
          if (discount_val > op.discount_low && discount_val < op.discount_high) {
            const double quantity_val = *((const double *)(row + quantity_field_offset));
            if (quantity_val < op.quantity) {
              result_bl.append(row + order_key_field_offset, 4);
              result_bl.append(row + line_number_field_offset, 4);
              add_extra_row_cost(op.extra_row_cost);
            }
          }
        }
      }
    } else {
      for (size_t rid = 0; rid < num_rows; rid++) {
        const char *row = rows + rid * row_size;

        const int shipdate_val = *((const int *)(row + shipdate_field_offset));
        if (shipdate_val >= op.ship_date_low && shipdate_val < op.ship_date_high) {
          const double discount_val = *((const double *)(row + discount_field_offset));
          if (discount_val > op.discount_low && discount_val < op.discount_high) {
            const double quantity_val = *((const double *)(row + quantity_field_offset));
            if (quantity_val < op.quantity) {
              result_bl.append(row, row_size);
              add_extra_row_cost(op.extra_row_cost);
            }
          }
        }
      }
    }
  } else if (op.query == "f") {
    if (op.projection) {
      RE2 re(op.comment_regex);
      for (size_t rid = 0; rid < num_rows; rid++) {
        const char *row = rows + rid * row_size;
        const char *cptr = row + comment_field_offset;
        const std::string comment_val = string_ncopy(cptr,
            comment_field_length);
        if (RE2::PartialMatch(comment_val, re)) {
          result_bl.append(row + order_key_field_offset, 4);
          result_bl.append(row + line_number_field_offset, 4);
          add_extra_row_cost(op.extra_row_cost);
        }
      }
    } else {
      RE2 re(op.comment_regex);
      for (size_t rid = 0; rid < num_rows; rid++) {
        const char *row = rows + rid * row_size;
        const char *cptr = row + comment_field_offset;
        const std::string comment_val = string_ncopy(cptr,
            comment_field_length);
        if (RE2::PartialMatch(comment_val, re)) {
          result_bl.append(row, row_size);
          add_extra_row_cost(op.extra_row_cost);
        }
      }
    }
  } else {
    return -EINVAL;
  }

  uint64_t eval_ns = getns() - eval_ns_start;

  ::encode(read_ns, *out);
  ::encode(eval_ns, *out);
  ::encode(result_bl, *out);

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

  cls_register_cxx_method(h_class, "query_op",
      CLS_METHOD_RD, query_op_op, &h_query_op);

  cls_register_cxx_method(h_class, "test_par",
      CLS_METHOD_RD, test_par, &h_test_par);

  cls_register_cxx_method(h_class, "build_index",
      CLS_METHOD_RD | CLS_METHOD_WR, build_index, &h_build_index);
}
