/*
* Copyright (C) 2018 The Regents of the University of California
* All Rights Reserved
*
* This library can redistribute it and/or modify under the terms
* of the GNU Lesser General Public License Version 2.1 as published
* by the Free Software Foundation.
*
*/

/*
 * The program starts in the main function. The function takes in a data file,
 * schema file, number of objects (aka buckets), number of rows till object
 * flushes, and total number of rows to be read. The main function then enters
 * a while loop till the specified number of lines are read from the data file.
 * In the while loop, each data row is parsed, processed, and hashed into a
 * bucket. If the number of rows till the bucket flushes is reached or all the
 * data rows have been read, then the contents of the bucket are "finished",
 * writen to disk, and the bucket is deleted.
*/

#include <fcntl.h>     // system call open
#include <iostream>
#include <fstream>
#include <sstream>
#include <map>
#include <unistd.h>    // for getOpt
#include <limits.h>
#include <boost/program_options.hpp>

#include "cls_tabular_utils.h"

using namespace std;
using namespace Tables;
namespace po = boost::program_options ;

const uint8_t SKYHOOK_VERSION = 1;
const uint8_t SCHEMA_VERSION = 1;
string SCHEMA = "";
uint64_t RID = 1;
typedef flatbuffers::FlatBufferBuilder fbBuilder;
typedef flatbuffers::FlatBufferBuilder* fbb;
typedef flexbuffers::Builder flxBuilder;
typedef vector<uint8_t> delete_vector;
typedef vector<flatbuffers::Offset<Record>> rows_vector;

typedef struct {
    uint64_t oid;
    uint64_t nrows;
    string table_name;
    fbb fb;
    delete_vector *deletev;
    rows_vector *rowsv;
} bucket_t;

//----------------- check inputs ------------------
std::vector<std::string> line_split(const std::string &s, char delim);
void promptDataFile(ifstream&, string&);
uint32_t promptIntVariable(string, string);
bool promptBoolVariable(string, string);
void helpMenu();

//-------------------------------------------------
Tables::schema_vec getSchema(vector<int>&, string&);

uint64_t getNextRID();

vector<string> parseRow(string row, char csv_delim);

void getFlxBuffer(flexbuffers::Builder *, vector<string>,
                  Tables::schema_vec, vector<uint64_t> *);

uint64_t hashCompositeKey(vector<int>, vector<string>);

uint64_t jumpConsistentHash(uint64_t, uint64_t);

bucket_t *retrieveBucketFromOID(map<uint64_t, bucket_t *> &, uint64_t, string);

void insertRowIntoBucket(fbb, uint64_t, vector<uint64_t> *, vector<uint8_t>,
                         delete_vector *, rows_vector *);

//------------- Finishing flatbuffer --------------
void flushFlatBuffer(uint8_t skyhook_v, uint8_t schema_v, bucket_t *bucketPtr,
                     string schema, uint64_t numOfObjs);

void finishFlatBuffer(fbb, uint8_t, uint8_t, string, string, delete_vector *,
                      rows_vector *, uint32_t);

int writeToDisk(uint64_t, uint8_t, bucket_t*, uint64_t);

void deleteBucket(bucket_t *bucketPtr, fbb fbPtr, delete_vector *deletePtr,
                  rows_vector *rowsPtr);

//-------------------------------------------------
vector<uint8_t> initializeFlexBuffer(vector<string> parsedRow,
                                     Tables::schema_vec schema,
                                     vector<uint64_t> *nullbits);



bucket_t *GetAndInitializeBucket(map<uint64_t, bucket_t *> &FBmap,
                                 uint64_t oid,
                                 vector<uint64_t> *nullbits,
                                 vector<uint8_t> flxPtr,
                                 string tablename);

int main(int argc, char *argv[])
{
    string file_name         = "";
    string schema_file_name  = "";
    string table_name        = "";
    uint64_t num_objs        = UINT_MAX;
    uint64_t rid_start_value = UINT_MAX;
    uint64_t flush_rows      = UINT_MAX;
    uint64_t read_rows       = UINT_MAX;
    uint64_t input_oid       = UINT_MAX;
    char csv_delim           = Tables::CSV_DELIM;
    bool use_hashing         = false;

// -------------- Get Variables ---------------
    po::options_description gen_opts("General options");
    gen_opts.add_options()
      ("help,h", "show help message")
      ("file_name", po::value<string>(&file_name)->required(), "file_name")
      ("schema_file_name", po::value<string>(&schema_file_name)->required(), "schema_file_name")
      ("num_objs", po::value<uint64_t>(&num_objs)->required(), "num_objs")
      ("rid_start_value", po::value<uint64_t>(&rid_start_value)->required(), "rid_start_value")
      ("flush_rows", po::value<uint64_t>(&flush_rows)->required(), "flush_rows")
      ("read_rows", po::value<uint64_t>(&read_rows)->required(), "read_rows")
      ("csv_delim", po::value<char>(&csv_delim)->required(), "csv_delim")
      ("use_hashing", po::value<bool>(&use_hashing)->required(), "use_hashing")
      ("table_name", po::value<string>(&table_name)->required(), "table_name")
      ("input_oid", po::value<uint64_t>(&input_oid)->required(), "input_oid");
  
    po::options_description all_opts("Allowed options");
    all_opts.add(gen_opts);
    po::variables_map vm;
    po::store( po::parse_command_line(argc, argv, all_opts ), vm);
    if(vm.count("help")) {
      std::cout << all_opts << std::endl;
      return 1;
    }
    po::notify(vm);
  
    // returns schema vector and composite keys
    Tables::schema_vec schema;
    vector<int> composite_key_indexes;
    schema = getSchema(composite_key_indexes, schema_file_name);
    SCHEMA = Tables::schemaToString(schema);

// ----------- Read Rows and Load into Corresponding FlatBuffer -----------
    map<uint64_t, bucket_t *> FBmap;
    bucket_t *bucketPtr;

    std::ifstream inFile(file_name);
    std::string line;
    uint64_t line_counter = 1;
    while(getline(inFile, line) && inFile.good()) {
        if((line_counter >= rid_start_value) &&
             (line_counter <= (rid_start_value+read_rows))) {
            auto parsedRow = parseRow(line, csv_delim);

            vector<uint64_t> *nullbits = new vector<uint64_t>(2,0);

            // --------- Get Row and Load into FlexBuffer ---------
            vector<uint8_t> flxPtr = initializeFlexBuffer(parsedRow,
                                                          schema,
                                                          nullbits);

            uint64_t oid     = -1 ;
            if(use_hashing) {
              // --------- Hash Composite Key ----------
              uint64_t hashKey = hashCompositeKey(composite_key_indexes, parsedRow);

              // --------- Get Oid Using HashKey ----------
              oid = jumpConsistentHash(hashKey, num_objs);
            }
            else {
              // write all rows between rid_start_row and (rid_start_row+read_rows)
              // to a single bucket/file.
              // define default oid
              oid  = input_oid ;
            }

            // --------- Get FB and insert ----------
            printf("Inserting Row %ld into Bucket %ld\n", line_counter, oid);
            bucketPtr = GetAndInitializeBucket(FBmap, oid, nullbits, flxPtr, table_name);

            // ----------- Flush if rows_flush was met -----------
            // TODO: either disable flushing when not using hashing 
            //       or increment the oid.
            //       otherwise, when flush_rows < read_rows,
            //       this will flush everything to the same object,
            //       which overwrites previously written data.
            if( bucketPtr->rowsv->size() >= flush_rows) {
                printf("\tFlushing bucket %ld to Ceph with %ld rows\n",
                       oid, bucketPtr->nrows);

                // Flush FlatBuffer to Ceph (currently writes to a file on disk)
                flushFlatBuffer(SKYHOOK_VERSION,
                                SCHEMA_VERSION,
                                bucketPtr,
                                SCHEMA,
                                num_objs);
                FBmap.erase(oid);
            } // if need to flush
            delete nullbits;
        } // if in rid range
        else if(line_counter >= (rid_start_value+read_rows))
             break ;
        else
            std::cout << "skipping row " << line_counter << std::endl ;

        line_counter++;
    } // while get a row

// ------------- Iterate over map and flush each bucket --------------
   if(use_hashing) {
        printf("\nFlush the remaining buckets in map\n");
        for (auto& x: FBmap) {
            bucket_t *b = x.second;
            printf("\tFlushing bucket %ld to Ceph with %ld rows\n",
                   b->oid, b->nrows);

            flushFlatBuffer(SKYHOOK_VERSION, SCHEMA_VERSION, b, SCHEMA, num_objs);
        } // for every FBmap key
    } // if using hashing

    FBmap.clear();
    printf("Done flushing all the objects\n");

    // Close .csv file
    if( inFile.is_open() )
        inFile.close();

    return 0;
}

std::vector<std::string> line_split(const std::string &s, char delim) {
    std::istringstream ss(s);
    std::string item;
    std::vector<std::string> tokens;
    while( getline(ss, item, delim)) {
        tokens.push_back(item);
    }
    return tokens;
}

void helpMenu() {
    printf("-h HELP MENU\n");
    printf("\t-f [file_name]\n");
    printf("\t-s [schema_file_name]\n");
    printf("\t-o [number_of_objects]\n");
    printf("\t-r [number_of_rows_until_flush]\n");
    printf("\t-i [rid_start_value]\n");
    printf("\t-n [number_of_rows_to_read]\n");
}

void promptDataFile(ifstream& inFile, string& file_name) {

    // Open File
    inFile.open(file_name, std::ifstream::in);

    // If file didn't open, prompt for data file
    while(!inFile)
    {
        cout<<"Cannot open file!"<<endl;
        cout<<"Which file will we load from? : ";
        getline(cin, file_name);
        inFile.open(file_name, ifstream::in);
    }
    std::cout<< "'" << file_name << "' was sucessfully opened!\n\n";
}

uint32_t promptIntVariable(string variable, string num_string) {
    stringstream myStream(num_string);
    uint32_t n = 0;

    while( !(myStream >> n) ) {
        std::cout << "How many " << variable << "?: ";
        getline(cin, num_string);
        stringstream newStream(num_string);
        if(newStream >> n)
            break;
        std::cout << "Invalid number, please try again\n";
    }
    std::cout << "Number of " << variable << ": " << n << endl << endl;
    return n;
}

bool promptBoolVariable(string variable, string bool_string) {
    stringstream myStream(bool_string);
    bool i = false ;

    while( !(myStream >> i) ) {
        std::cout << "Use hashing strategy? (true || false) " << variable << "?: ";
        getline(cin, bool_string);
        stringstream newStream(bool_string);
        if(newStream >> i)
            break;
        std::cout << "Invalid number, please try again\n";
    }
    std::cout << "Use hashing strategy " << variable << ": " << i << endl << endl;
    return i;
}

vector<string> parseRow(string row, char delim=Tables::CSV_DELIM) {
    return line_split(row, delim);
}

Tables::schema_vec getSchema(vector<int>& compositeKey,
                             string& schema_file_name) {
    ifstream schemaFile;

    vector<int> schema;

    // Try to open schema file
    schemaFile.open(schema_file_name, std::ifstream::in);

    // If schema file didn't open, prompt for schema file name
    while(!schemaFile)
    {
        if(strcmp(schema_file_name.c_str(),"q") == 0 or
           strcmp(schema_file_name.c_str(),"Q") == 0) {
            exit(0);
        }

        std::cout << "Cannot open file! \t\t(Press Q or q to quit program)\n";
        std::cout << "Which file will we get the schema from? : ";
        getline(cin, schema_file_name);
        schemaFile.open(schema_file_name, ifstream::in);
    }

    // Parse Schema File for Composite Key
    string line = "";
    vector<string> splitLine;
    // Parse Schema File for Data Types
    while( getline(schemaFile,line) ) {
        splitLine = line_split(line, ' ');
        if ( stoi(splitLine[2].c_str()) == 1 ) {
            compositeKey.push_back(stoi(splitLine[0].c_str()));
        }
    }
    schemaFile.close();

    // Getting entire schema string
    ifstream t(schema_file_name);
    stringstream buffer;
    buffer << t.rdbuf();
    string schema_str = buffer.str();
    Tables::schema_vec sky_schema = Tables::schemaFromString(schema_str);
    std::cout << "'" << schema_file_name
              << "' was sucessfully read and schema vector passed back!\n\n";
    return sky_schema;
}

vector<uint8_t>
initializeFlexBuffer(vector<string> parsedRow,
                     Tables::schema_vec schema,
                     vector<uint64_t> *nullbits) {

    flexbuffers::Builder *flx = new flexbuffers::Builder();

    // load parsed row into our flxBuilder and update nullbits
    getFlxBuffer(flx, parsedRow, schema, nullbits);

    // get pointer to FlexBuffer
    vector<uint8_t> flxPtr = flx->GetBuffer();
    delete flx;
    return flxPtr;
}

void getFlxBuffer(flxBuilder *flx,
                  vector<string> parsedRow,
                  Tables::schema_vec schema,
                  vector<uint64_t> *nullbits) {

    bool nullFlag = false;
    string nullcmp = "NULL";

    // Create Flexbuffer from Parsed Row and Schema
    flx->Vector([&]() {
        for(int i=0;i<(int)schema.size();i++) {
            Tables::col_info col = schema[i];
            nullFlag = false;
            if(strcmp(parsedRow[i].c_str(), nullcmp.c_str() ) == 0)
                nullFlag = true;
            if(nullFlag) {
                // Mark nullbit
                uint64_t nullMask = 0x00;
                if(i<64) {
                    nullMask = 1lu << (63-i);
                    nullbits[0][0] |=  nullMask;
                }
                else {
                    nullMask = 1lu << (63- (i-64));
                    nullbits[0][1] |= nullMask;
                }

                printf("NULL CASE SWITCHES \n");
                // Put a dummy variable to hold the index for future updates
                switch(col.type) {
                case Tables::SDT_INT8:
                    flx->Add(static_cast<int8_t>(0));
                    break;
                case Tables::SDT_INT16:
                    flx->Add(static_cast<int16_t>(0));
                    break;
                case Tables::SDT_INT32:
                    flx->Add(static_cast<int32_t>(0));
                    break;
                case Tables::SDT_INT64:
                    flx->Add(static_cast<int64_t>(0));
                    break;
                case Tables::SDT_UINT8:
                    flx->Add(static_cast<uint8_t>(0));
                    break;
                case Tables::SDT_UINT16:
                    flx->Add(static_cast<uint16_t>(0));
                    break;
                case Tables::SDT_UINT32:
                    flx->Add(static_cast<uint32_t>(0));
                    break;
                case Tables::SDT_UINT64:
                    flx->Add(static_cast<uint64_t>(0));
                    break;
                case Tables::SDT_CHAR:
                    flx->Add(static_cast<char>(0));
                    break;
                case Tables::SDT_UCHAR:
                    flx->Add(static_cast<unsigned char>(0));
                    break;
                case Tables::SDT_BOOL:
                    flx->Add(false);
                    break;
                case Tables::SDT_FLOAT:
                    flx->Add(static_cast<float>(0));
                    break;
                case Tables::SDT_DOUBLE:
                    flx->Add(static_cast<double>(0));
                    break;
                case Tables::SDT_DATE:
                    flx->Add("0000-00-00");
                    break;
                case Tables::SDT_STRING:
                    flx->Add("This will be pooled with strings.");
                    break;
                default:
                    flx->Add("EMPTY");
                    break;
                }
            }
            else {
                switch(col.type) {
                case Tables::SDT_INT8:
                    flx->Add(static_cast<int8_t>(stoi(parsedRow[col.idx].c_str())));
                    break;
                case Tables::SDT_INT16:
                    flx->Add(static_cast<int16_t>(stoi(parsedRow[col.idx].c_str())));
                    break;
                case Tables::SDT_INT32:
                    flx->Add(static_cast<int32_t>(stoi(parsedRow[col.idx].c_str())));
                    break;
                case Tables::SDT_INT64:
                    flx->Add(static_cast<int64_t>(stoi(parsedRow[col.idx].c_str())));
                    break;
                case Tables::SDT_UINT8:
                    flx->Add(static_cast<uint8_t>(stoul(parsedRow[col.idx].c_str())));
                    break;
                case Tables::SDT_UINT16:
                    flx->Add(static_cast<uint16_t>(stoul(parsedRow[col.idx].c_str())));
                    break;
                case Tables::SDT_UINT32:
                    flx->Add(static_cast<uint32_t>(stoul(parsedRow[col.idx].c_str())));
                    break;
                case Tables::SDT_UINT64:
                    flx->Add(static_cast<uint64_t>(stoul(parsedRow[col.idx].c_str())));
                    break;
                case Tables::SDT_CHAR:
                    flx->Add(static_cast<char>(parsedRow[col.idx][0]));
                    break;
                case Tables::SDT_UCHAR:
                    flx->Add(static_cast<unsigned char>(parsedRow[col.idx][0]));
                    break;
                case Tables::SDT_BOOL:
                    flx->Add(static_cast<bool>(parsedRow[col.idx].c_str()));
                    break;
                case Tables::SDT_FLOAT:
                    flx->Add(static_cast<float>(stof(parsedRow[col.idx].c_str())));
                    break;
                case Tables::SDT_DOUBLE:
                    flx->Add(static_cast<double>(stod(parsedRow[col.idx].c_str())));
                    break;
                case Tables::SDT_DATE:
                    flx->Add(parsedRow[col.idx].c_str());
                    break;
                case Tables::SDT_STRING:
                    flx->Add(parsedRow[col.idx].c_str());
                    break;
                default:
                    flx->Add("EMPTY");
                    break;
                }
            }
        }
    });
    flx->Finish();
}

uint64_t hashCompositeKey(vector<int> compositeKeyIndexes,
                          vector<string> parsedRow) {

    // Hash the Composite Key
    uint64_t hashKey=0, upper=0, lower=0;
    stringstream(parsedRow[compositeKeyIndexes[0]]) >> upper;
    hashKey = upper << 32;
    if(compositeKeyIndexes.size() > 1) {
        stringstream(parsedRow[compositeKeyIndexes[1]]) >> lower;
        hashKey = hashKey | lower;
    }
    if (compositeKeyIndexes.size() > 2)
        assert (Tables::TablesErrCodes::UnsupportedNumKeyCols==0);
    return hashKey;
}

uint64_t jumpConsistentHash(uint64_t key, uint64_t num_buckets) {
    // Source:
    // A Fast, Minimal Memory, Consistent Hash Algorithm
    // https://arxiv.org/ftp/arxiv/papers/1406/1406.2294.pdf

    int64_t b=-1l, j=0l;
    while(j < (int64_t)num_buckets) {
        b = j;
        key = key * 286293355588894185ULL + 1;
        j = (b+1) * (double(1LL << 31) / double((key>>33) + 1));
    }
    return b;
}

bucket_t* GetAndInitializeBucket(
    map<uint64_t, bucket_t *> &FBmap,
    uint64_t oid,vector<uint64_t> *nullbits,
    vector<uint8_t> flxPtr,
    string tablename) {

    bucket_t *bucketPtr;
    bucketPtr = retrieveBucketFromOID(FBmap, oid, tablename);

    fbb fbPtr = bucketPtr->fb;
    delete_vector *deletePtr;
    rows_vector *rowsPtr;
    deletePtr = bucketPtr->deletev;
    rowsPtr = bucketPtr->rowsv;

    uint64_t RID = getNextRID();

    insertRowIntoBucket(fbPtr, RID, nullbits, flxPtr, deletePtr, rowsPtr);
    bucketPtr->nrows++;
    return bucketPtr;
}

bucket_t*
retrieveBucketFromOID(map<uint64_t, bucket_t *> &FBmap, uint64_t oid, string tablename) {

    bucket_t *bucketPtr;
    // Get FB from map or use new FB
    map<uint64_t, bucket_t *>::iterator it;
    it = FBmap.find(oid);
    if(it != FBmap.end()) {
        bucketPtr = it->second;
    }
    else
    {
        bucketPtr = new bucket_t();
        bucketPtr->oid = oid;
        bucketPtr->nrows = 0;
        bucketPtr->table_name = tablename;
        bucketPtr->fb = new fbBuilder();
        bucketPtr->deletev = new delete_vector();
        bucketPtr->rowsv = new rows_vector();
        FBmap[oid] = bucketPtr;
    }
    return bucketPtr;
}

void
insertRowIntoBucket(
    fbb fbPtr,
    uint64_t RID,
    vector<uint64_t> *nullbits,
    vector<uint8_t> flxPtr,
    delete_vector *deletePtr,
    rows_vector *rowsPtr) {

    // Serialize FlexBuffer row into FlatBufferBuilder
    auto flxSerial = fbPtr->CreateVector(flxPtr);
    auto nullbitsSerial = fbPtr->CreateVector(*nullbits);
    auto rowOffset = CreateRecord(*fbPtr, RID, nullbitsSerial, flxSerial);
    deletePtr->push_back(0);
    rowsPtr->push_back(rowOffset);
}

// NOTE: this is a blind increment, should come from database
uint64_t getNextRID() {
    return RID++;
}

void
flushFlatBuffer(
    uint8_t skyhook_v,
    uint8_t schema_v,
    bucket_t *bucketPtr,
    string schema,
    uint64_t numOfObjs) {

    delete_vector *deletePtr;
    rows_vector *rowsPtr;

    fbb fbPtr  = bucketPtr->fb;
    deletePtr  = bucketPtr->deletev;
    rowsPtr    = bucketPtr->rowsv;
    string table_name = bucketPtr->table_name;

    // -----------------------------------
    // Finish FlatBuffer
    // had to pull this out of a method bc it wouldn't compile???

    auto data_format_type = skyhook_v;
    auto skyhook_version = 2; //skyhook_v2
    auto data_structure_version = schema_v;
    auto data_schema_version = schema_v;
    auto data_schema = fbPtr->CreateString(schema);
    auto db_schema = fbPtr->CreateString("*");
    auto table_n = fbPtr->CreateString(table_name);
    auto delete_vector = fbPtr->CreateVector(*deletePtr);
    auto rows = fbPtr->CreateVector(*rowsPtr);

    auto tableOffset = \
        CreateTable(*fbPtr,
                    data_format_type,
                    skyhook_version,
                    data_structure_version,
                    data_schema_version,
                    data_schema,
                    db_schema,
                    table_n,
                    delete_vector,
                    rows,
                    bucketPtr->nrows);

    fbPtr->Finish(tableOffset);

    // -----------------------------------

    uint64_t oid = bucketPtr->oid;

    // Flush to Ceph Here TO OID bucket with n Rows or Crash if Failed
    if(writeToDisk(oid, schema_v, bucketPtr, numOfObjs) < 0)
        exit(EXIT_FAILURE);

    // Deallocate pointers
    deleteBucket(bucketPtr, fbPtr, deletePtr, rowsPtr);
}


// TODO: set default version numbers to version fields
// (i.e. data_structure_type, fb_version, data_structure_version).
void finishFlatBuffer(
        fbb fbPtr,
        uint8_t skyhook_v,
        uint8_t schema_v,
        string table_name,
        string schema,
        delete_vector *deletePtr,
        rows_vector *rowsPtr,
        uint64_t nrows)
{

    auto data_format_type = skyhook_v;
    auto skyhook_version = 2; //skyhook_v2
    auto data_structure_version = schema_v;
    auto data_schema_version = schema_v;
    auto data_schema = fbPtr->CreateString(schema);
    auto db_schema = fbPtr->CreateString("*");
    auto table_n = fbPtr->CreateString(table_name);
    auto delete_vector = fbPtr->CreateVector(*deletePtr);
    auto rows = fbPtr->CreateVector(*rowsPtr);

    auto tableOffset = \
        CreateTable(*fbPtr,
                    data_format_type,
                    skyhook_version,
                    data_structure_version,
                    data_schema_version,
                    data_schema,
                    db_schema,
                    table_n,
                    delete_vector,
                    rows,
                    nrows);

    fbPtr->Finish(tableOffset);
}


/* TODO: instead of writing objects to disk, write directly to ceph. */
int
writeToDisk(
    uint64_t oid,
    uint8_t schema_v,
    bucket_t *bucket,
    uint64_t nobjs) {

    // NOTE: assumes bucketPtr->fb has been finished(), i.e., points to
    // formatted, serialized data

    // CREATE An FB_META, using an empty builder first.
    flatbuffers::FlatBufferBuilder *meta_builder = \
            new flatbuffers::FlatBufferBuilder();
    createFbMeta(
            meta_builder,
            SFT_FLATBUF_FLEX_ROW,
            reinterpret_cast<unsigned char*>(bucket->fb->GetBufferPointer()),
            bucket->fb->GetSize());

    // add meta_builder's data into a bufferlist as char*
    bufferlist meta_bl;
    meta_bl.append(
            reinterpret_cast<const char*>(meta_builder->GetBufferPointer()),
            meta_builder->GetSize());

    // now encode the metabl into a wrapper bl so that it can be easily
    // unpacked via decode from a bl iterator into a bl, used by
    // query.cc and cls_tabular.cc
    bufferlist bl_wrapper;
    ::encode(meta_bl, bl_wrapper);

    string fname = "fbmeta.Skyhook.v2." + bucket->table_name
                                        + "." + std::to_string(oid)
                                        + ".1-" + std::to_string(nobjs);

    // write to disk as binary bl data.
    int mode = 0600;
    bl_wrapper.write_file(fname.c_str(), mode);
    std::cout << "bucket->fb->GetSize()=" << bucket->fb->GetSize()
              << "; fbmeta_size=" << meta_builder->GetSize()
              << "; meta_bl.len=" << meta_bl.length()
              << "; bl_wrapper.len=" << bl_wrapper.length()
              << std::endl;

    delete meta_builder;
    return 0;
}

void
deleteBucket(
    bucket_t *bucketPtr,
    fbb fbPtr,
    delete_vector *deletePtr,
    rows_vector *rowsPtr) {

    printf("Clearing FB Ptr and RowsVector Ptr, Delete Bucket from Map\n\n");
    fbPtr->Reset();
    delete fbPtr;
    deletePtr->clear();
    delete deletePtr;
    rowsPtr->clear();
    delete rowsPtr;
    delete bucketPtr;
}
