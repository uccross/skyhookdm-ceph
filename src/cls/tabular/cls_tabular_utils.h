/*
* Copyright (C) 2018 The Regents of the University of California
* All Rights Reserved
*
* This library can redistribute it and/or modify under the terms
* of the GNU Lesser General Public License Version 2.1 as published
* by the Free Software Foundation.
*
*/


#ifndef CLS_TABULAR_UTILS_H
#define CLS_TABULAR_UTILS_H

#include <type_traits>

#include "include/types.h"
#include <boost/algorithm/string/classification.hpp> // for boost::is_any_of
#include <boost/algorithm/string/split.hpp> // for boost::split
#include <boost/algorithm/string.hpp> // for boost::trim
#include "re2/re2.h"

#include "cls_tabular.h"
#include "flatbuffers/flexbuffers.h"
#include "skyhookv1_generated.h"

namespace Tables {

enum TablesErrCodes {
    EmptySchema = 1, // note: must start at 1
    BadColInfoFormat,
    BadColInfoConversion,
    UnsupportedSkyDataType,
    UnsupportedAggDataType,
    UnknownSkyDataType,
    RequestedColIndexOOB,
    PredicateComparisonNotDefined,
    PredicateRegexPatternNotSet,
    OpNotRecognized,
    OpNotImplemented,
};

// skyhook data types, as supported by underlying data format
enum SkyDataType {
    SKY_INT8 = 1,  // note: must start at 1
    SKY_INT16,
    SKY_INT32,
    SKY_INT64,
    SKY_UINT8,
    SKY_UINT16,
    SKY_UINT32,
    SKY_UINT64,
    SKY_CHAR,
    SKY_UCHAR,
    SKY_BOOL,
    SKY_FLOAT,
    SKY_DOUBLE,
    SKY_DATE,
    SKY_STRING,
    /*
    SKY_BLOB,  // TODO
    SKY_VECTOR_INT8,  // TODO
    SKY_VECTOR_INT64,  // TODO
    SKY_VECTOR_UINT8,  // TODO
    SKY_VECTOR_UINT64,  // TODO
    SKY_VECTOR_DOUBLE,  // TODO
    SKY_VECTOR_STRING,  // TODO
    */
    SkyDataType_FIRST = SKY_INT8,
    SkyDataType_LAST = SKY_STRING,
};

enum SkyOpType {
    // ARITHMETIC COMPARISON
    lt = 1,  // note: must start at 1
    gt,
    eq,
    ne,
    leq,
    geq,
    // ARITHMETIC FUNCTIONS (TODO)
    add,
    sub,
    mul,
    div,
    min,
    max,
    sum,
    cnt,
    // LEXICAL (regex)
    like,
    // MEMBERSHIP (collections) (TODO)
    in,
    not_in,
    // DATE (SQL)  (TODO)
    between,
    // LOGICAL
    logical_or,
    logical_and,
    logical_not,
    logical_nor,
    logical_xor,
    logical_nand,
    // BITWISE
    bitwise_and,
    bitwise_or,
    SkyOpType_FIRST = lt,
    SkyOpType_LAST = bitwise_or,
};

enum AggIdx {
    AGG_MIN = -1,  // defines the schema idx for agg ops.
    AGG_MAX = -2,
    AGG_SUM = -3,
    AGG_CNT = -4
};

const std::map<std::string, int> agg_idx_names = {
    {"min", AGG_MIN},
    {"max", AGG_MAX},
    {"sum", AGG_SUM},
    {"cnt", AGG_CNT}
};

const int offset_to_skyhook_version = 4;
const int offset_to_schema_version = 6;
const int offset_to_table_name = 8;
const int offset_to_schema = 10;
const int offset_to_delete_vec = 12;
const int offset_to_rows_vec = 14;
const int offset_to_nrows = 16;
const int offset_to_RID = 4;
const int offset_to_nullbits_vec = 6;
const int offset_to_data = 8;
const std::string PRED_DELIM_OUTER = ";";
const std::string PRED_DELIM_INNER = ",";
const std::string PROJECT_DEFAULT = "*";
const std::string SELECT_DEFAULT = "*";
const std::string REGEX_DEFAULT_PATTERN = "/.^/";  // matches nothing.

// contains the value of a predicate to be applied
template <class T>
class PredicateValue
{
public:
    T val;
    PredicateValue(T v) : val(v) {};
    PredicateValue(const PredicateValue& rhs);
    PredicateValue& operator=(const PredicateValue& rhs);
};

// PredBase is not template typed, derived is type templated,
// allows us to have vectors of chained base class predicates
class PredicateBase
{
public:
    virtual ~PredicateBase() {}
    virtual int colIdx() = 0;  // to check info via base ptr before dynm cast
    virtual int colType() = 0;
    virtual int opType() = 0;
    virtual bool isGlobalAgg() = 0;
};
typedef std::vector<class PredicateBase*> predicate_vec;

template <typename T>
class TypedPredicate : public PredicateBase
{
private:
    const int col_idx;
    const int col_type;
    const int op_type;
    const bool is_global_agg;
    const re2::RE2* regx;
    const PredicateValue<T> value;
    PredicateValue<T> agg_value;  // inits to same as value

public:
    TypedPredicate(int idx, int type, int op, const T& val) :
        col_idx(idx),
        col_type(type),
        op_type(op),
        is_global_agg(op==min || op==max || op==sum || op==cnt),
        value(val),
        agg_value(val) {

            // ONLY VERIFY op type is valid for specified col type and value
            // type T, and compile regex if needed.
            switch (op_type) {
                // ARITHMETIC/COMPARISON
                case lt:
                case gt:
                case eq:
                case ne:
                case leq:
                case geq:
                case add:
                case sub:
                case mul:
                case div:
                case min:
                case max:
                case sum:
                case cnt:
                    assert (
                            std::is_arithmetic<T>::value
                            && (
                            col_type==SKY_INT8 ||
                            col_type==SKY_INT16 ||
                            col_type==SKY_INT32 ||
                            col_type==SKY_INT64 ||
                            col_type==SKY_UINT8 ||
                            col_type==SKY_UINT16 ||
                            col_type==SKY_UINT32 ||
                            col_type==SKY_UINT64 ||
                            col_type==SKY_BOOL ||
                            col_type==SKY_CHAR ||
                            col_type==SKY_UCHAR ||
                            col_type==SKY_FLOAT ||
                            col_type==SKY_DOUBLE));
                    break;

                // LEXICAL (regex)
                case like:
                    assert ((
                        (std::is_same<T, std::string>::value) == true ||
                        (std::is_same<T, unsigned char>::value) == true ||
                        (std::is_same<T, char>::value) == true)
                        && (
                        col_type==SKY_STRING ||
                        col_type==SKY_CHAR ||
                        col_type==SKY_UCHAR));
                    break;

                // MEMBERSHIP (collections)
                case in:
                case not_in:
                    assert(((std::is_same<T, std::vector<T>>::value) == true));
                    assert (TablesErrCodes::OpNotImplemented==0); // TODO
                    break;

                // DATE (SQL)
                case between:
                    assert (TablesErrCodes::OpNotImplemented==0);  // TODO
                    break;

                // LOGICAL
                case logical_or:
                case logical_and:
                case logical_not:
                case logical_nor:
                case logical_xor:
                case logical_nand:
                    assert (std::is_integral<T>::value);  // includes bool+char
                    assert (col_type==SKY_INT8 ||
                            col_type==SKY_INT16 ||
                            col_type==SKY_INT32 ||
                            col_type==SKY_INT64 ||
                            col_type==SKY_UINT8 ||
                            col_type==SKY_UINT16 ||
                            col_type==SKY_UINT32 ||
                            col_type==SKY_UINT64 ||
                            col_type==SKY_BOOL ||
                            col_type==SKY_CHAR ||
                            col_type==SKY_UCHAR);
                    break;

                // BITWISE
                case bitwise_and:
                case bitwise_or:
                    assert (std::is_integral<T>::value);
                    assert (std::is_unsigned<T>::value);
                    break;

                // FALL THROUGH OP not recognized
                default:
                    assert (TablesErrCodes::OpNotRecognized==0);
                    break;
            }

            // compile regex
            std::string pattern;
            if (op_type == SkyOpType::like) {
                pattern = this->getVal();  // force str type for regex
                regx = new re2::RE2(pattern);
                assert (regx->ok());
            }
        }

    TypedPredicate(const TypedPredicate &p) : // copy constructor as needed
        col_idx(p.col_idx),
        col_type(p.col_type),
        op_type(p.op_type),
        is_global_agg(p.is_global_agg),
        value(p.value.val),
        agg_value(p.agg_value.val) {
            regx = new re2::RE2(p.regx->pattern());
        }

    ~TypedPredicate() { }
    TypedPredicate& getThis() {return *this;}
    const TypedPredicate& getThis() const {return *this;}
    virtual int colIdx() {return col_idx;}
    virtual int colType() {return col_type;}
    virtual int opType() {return op_type;}
    virtual bool isGlobalAgg() {return is_global_agg;}
    T getVal() {return value.val;}
    const re2::RE2* getRegex() {return regx;}
    void updateAgg(int64_t newval) {agg_value.val = newval;}
    void updateAgg(uint64_t newval) {agg_value.val = newval;}
    void updateAgg(double newval) {agg_value.val = newval;}
    T getAgg() {return agg_value.val;}
};

// col metadata used for the schema
struct col_info {
    const int idx;
    const int type;
    const bool is_key;
    const bool nullable;
    const std::string name;

    col_info(int i, int t, bool key, bool nulls, std::string n) :
        idx(i),
        type(t),
        is_key(key),
        nullable(nulls),
        name(n) {assert(type >= SkyDataType_FIRST && type <= SkyDataType_LAST);}

    col_info(std::string i, std::string t, std::string key, std::string nulls,
        std::string n) :
        idx(std::stoi(i.c_str())),
        type(std::stoi(t.c_str())),
        is_key(key[0]=='1'),
        nullable(nulls[0]=='1'),
        name(n) {assert(type >= SkyDataType_FIRST && type <= SkyDataType_LAST);}

    std::string toString() {
        return ( "   " +
            std::to_string(idx) + " " +
            std::to_string(type) + " " +
            std::to_string(is_key) + " " +
            std::to_string(nullable) + " " +
            name + "   ");}

    bool compareName(std::string colname) {return (colname==name)?true:false;}
};
typedef vector<struct col_info> schema_vec;

inline
bool compareColInfo(const struct col_info& l, const struct col_info& r) {
    return (
        (l.idx==r.idx) &&
        (l.type==r.type) &&
        (l.is_key==r.is_key) &&
        (l.nullable==r.nullable) &&
        (l.name==r.name)
    );
}

// the below are used in our root table
typedef vector<uint8_t> delete_vector;
typedef const flatbuffers::Vector<flatbuffers::Offset<Row>>* row_offs;

// the below are used in our row table
typedef vector<uint64_t> nullbits_vector;
typedef flexbuffers::Reference row_data_ref;

// skyhookdb root metadata, refering to a (sub)partition of rows
// abstracts a partition from its underlying data format/layout
struct root_table {
    const int skyhook_version;
    int schema_version;
    string table_name;
    string schema;
    delete_vector delete_vec;
    row_offs offs;
    uint32_t nrows;

    root_table(int skyver, int schmver, std::string tblname, std::string schm,
               delete_vector d, row_offs ro, uint32_t n) :
        skyhook_version(skyver),
        schema_version(schmver),
        table_name(tblname),
        schema(schm),
        delete_vec(d),
        offs(ro),
        nrows(n) {};

};
typedef struct root_table sky_root_header;

// skyhookdb row metadata and row data, wraps a row of data
// abstracts a row from its underlying data format/layout
struct row_table {
    const int64_t RID;
    nullbits_vector nullbits;
    const row_data_ref data;

    row_table(int64_t rid, nullbits_vector n, row_data_ref d) :
        RID(rid),
        nullbits(n),
        data(d) {};
};
typedef struct row_table sky_row_header;

// a test schema for the tpch lineitem table.
// format: "col_idx col_type col_is_key nullable col_name \n"
// note the col_idx always refers to the index in the table's current schema
const std::string lineitem_test_schema_string = " \
    0 " +  std::to_string(SKY_INT64) + " 1 0 orderkey \n\
    1 " +  std::to_string(SKY_INT32) + " 0 1 partkey \n\
    2 " +  std::to_string(SKY_INT32) + " 0 1 suppkey \n\
    3 " +  std::to_string(SKY_INT64) + " 1 0 linenumber \n\
    4 " +  std::to_string(SKY_FLOAT) + " 0 1 quantity \n\
    5 " +  std::to_string(SKY_DOUBLE) + " 0 1 extendedprice \n\
    6 " +  std::to_string(SKY_FLOAT) + " 0 1 discount \n\
    7 " +  std::to_string(SKY_DOUBLE) + " 0 1 tax \n\
    8 " +  std::to_string(SKY_CHAR) + " 0 1 returnflag \n\
    9 " +  std::to_string(SKY_CHAR) + " 0 1 linestatus \n\
    10 " +  std::to_string(SKY_DATE) + " 0 1 shipdate \n\
    11 " +  std::to_string(SKY_DATE) + " 0 1 commitdate \n\
    12 " +  std::to_string(SKY_DATE) + " 0 1 receipdate \n\
    13 " +  std::to_string(SKY_STRING) + " 0 1 shipinstruct \n\
    14 " +  std::to_string(SKY_STRING) + " 0 1 shipmode \n\
    15 " +  std::to_string(SKY_STRING) + " 0 1 comment \n\
    ";

// a test schema for procection over the tpch lineitem table.
// format: "col_idx col_type col_is_key nullable col_name \n"
// note the col_idx always refers to the index in the table's current schema
const std::string lineitem_test_project_schema_string = " \
    0 " +  std::to_string(SKY_INT64) + " 1 0 orderkey \n\
    1 " +  std::to_string(SKY_INT32) + " 0 1 partkey \n\
    3 " +  std::to_string(SKY_INT64) + " 1 0 linenumber \n\
    4 " +  std::to_string(SKY_FLOAT) + " 0 1 quantity \n\
    5 " +  std::to_string(SKY_DOUBLE) + " 0 1 extendedprice \n\
    ";

// these extract the current data format (flatbuf) into the skyhookdb
// root table and row table data structure defined above, abstracting
// skyhookdb data partitions design from the underlying data format.
sky_root_header getSkyRootHeader(const char *fb, size_t fb_size);
sky_row_header getSkyRowHeader(const Tables::Row *rec);

// print functions (debug only)
void printSkyRootHeader(sky_root_header *r);
void printSkyRowHeader(sky_row_header *r);
void printSkyFb(const char* fb, size_t fb_size, schema_vec &schema);

// convert provided schema to/from skyhook internal representation
void schemaFromProjectColsString(schema_vec &ret_schema,
                                 schema_vec &current_schema,
                                 std::string project_col_names);
void schemaFromString(schema_vec &schema, std::string schema_string);
std::string schemaToString(schema_vec schema);

// convert provided predicates to/from skyhook internal representation
void predsFromString(predicate_vec &preds,  schema_vec &schema,
                     std::string preds_string);
std::string predsToString(predicate_vec &preds,  schema_vec &schema);

bool hasAggPreds(predicate_vec &preds);

// convert provided ops to/from internal skyhook representation (simple enums)
int skyOpTypeFromString(std::string s);
std::string skyOpTypeToString(int op);

// for proj, select, fastpath, aggregations(TODO), build return fb
int processSkyFb(
        flatbuffers::FlatBufferBuilder& flatb,
        schema_vec& schema_in,
        schema_vec& schema_out,
        predicate_vec& preds,
        const char* fb,
        const size_t fb_size,
        std::string& errmsg);

inline
bool applyPredicates(predicate_vec& pv, sky_row_header& row);

inline
bool compare(const int64_t& val1, const int64_t& val2, const int& op);

inline
bool compare(const uint64_t& val1, const uint64_t& val2, const int& op);

inline
bool compare(const double& val1, const double& val2, const int& op);

inline
bool compare(const bool& val1, const bool& val2, const int& op);

inline
bool compare(const std::string& val1, const re2::RE2& regx, const int& op);

inline
int64_t computeAgg(const int64_t& val, const int64_t& oldval, const int& op);

inline
uint64_t computeAgg(const uint64_t& val, const uint64_t& oldval, const int& op);

inline
double computeAgg(const double& val, const double& oldval, const int& op);

} // end namespace Tables

#endif
