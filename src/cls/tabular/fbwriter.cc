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
//#include "skyhookv2_generated.h"

#include "cls_tabular_utils.h"

using namespace std;
using namespace Tables;

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
	uint32_t nrows;
	string table_name;
	fbb fb;
	delete_vector *deletev;
	rows_vector *rowsv;
} bucket_t;

//----------------- check inputs ------------------
std::vector<std::string> split(const std::string &s, char delim);
void promptDataFile(ifstream&, string&);
uint32_t promptIntVariable(string, string);
void helpMenu();
//-------------------------------------------------
Tables::schema_vec getSchema(vector<int>&, string&);
uint64_t getNextRID();
vector<string> getNextRow(ifstream& inFile);
void getFlxBuffer(flexbuffers::Builder *, vector<string>, Tables::schema_vec, vector<uint64_t> *);
uint64_t hashCompositeKey(vector<int>, vector<string>);
uint64_t jumpConsistentHash(uint64_t, uint64_t);
bucket_t *retrieveBucketFromOID(map<uint64_t, bucket_t *> &, uint64_t);
void insertRowIntoBucket(fbb, uint64_t, vector<uint64_t> *, vector<uint8_t>, delete_vector *, rows_vector *);
//------------- Finishing flatbuffer --------------
void flushFlatBuffer(uint8_t skyhook_v, uint8_t schema_v, bucket_t *bucketPtr, string schema, uint64_t numOfObjs);
void finishFlatBuffer(fbb, uint8_t, uint8_t, string, string, delete_vector *, rows_vector *, uint32_t);
int writeToDisk(uint64_t, uint8_t, bucket_t*, uint64_t);
void deleteBucket(bucket_t *bucketPtr, fbb fbPtr, delete_vector *deletePtr, rows_vector *rowsPtr);
//-------------------------------------------------
vector<uint8_t> initializeFlexBuffer(vector<string> parsedRow, Tables::schema_vec schema,vector<uint64_t> *nullbits);
bucket_t *GetAndInitializeBucket(map<uint64_t, bucket_t *> &FBmap,uint64_t oid,vector<uint64_t> *nullbits,vector<uint8_t> flxPtr);

int main(int argc, char *argv[])
{
	ifstream inFile;
	string file_name = "";
	string schema_file_name = "";
	vector<int> composite_key_indexes; 
	Tables::schema_vec schema;
	uint64_t num_objs = UINT_MAX;
	uint32_t flush_rows = UINT_MAX;
	uint32_t read_rows = UINT_MAX;
// -------------- Verify Configurable Variables or Prompt For Them ---------------
	int opt;
	while( (opt = getopt(argc, argv, "hf:s:o:r:n:i:")) != -1) {
		switch(opt) {
			case 'f':
				// Open .csv file
				file_name = optarg;
				promptDataFile(inFile, file_name);
				break;
			case 's':
				// Get Schema (vector of enum types), schema file name, composite key
				schema_file_name = optarg;
				schema = getSchema(composite_key_indexes, schema_file_name);	// returns schema vector and composite keys
				SCHEMA = Tables::schemaToString(schema);
				break;
			case 'o':
				// Set # of Objects
				num_objs = promptIntVariable("objects", optarg);
				break;
			case 'i':
				RID = promptIntVariable("RID start value", optarg);
		   		break;
			case 'r':
				// Set # of Rows to Read Until Flushed
				flush_rows = promptIntVariable("rows until flush", optarg);
				break;
			case 'n':
				// Set # of Total Rows to Read
				read_rows = promptIntVariable("rows to read", optarg);
				break;
			case 'h':
				helpMenu();
				exit(0);
				break;
			default:
				std::cout<<"Opt "<<opt<<" not valid"<<endl;
		}
	}
// ----------- Read Rows and Load into Corresponding FlatBuffer -----------
	map<uint64_t, bucket_t *> FBmap;
	bucket_t *bucketPtr;
	
	uint32_t rows_loaded_into_fb = 0;
	vector<string> parsedRow = getNextRow(inFile);
	
	while( (rows_loaded_into_fb < read_rows) && (!inFile.eof()) ) {
		
		vector<uint64_t> *nullbits = new vector<uint64_t>(2,0);
		
		// --------- Get Row and Load into FlexBuffer ---------
		vector<uint8_t> flxPtr = initializeFlexBuffer(parsedRow,schema,nullbits);
		
		// --------- Hash Composite Key ----------
		uint64_t hashKey = hashCompositeKey(composite_key_indexes, parsedRow);

		// --------- Get Oid Using HashKey ----------
 		uint64_t oid = jumpConsistentHash(hashKey, num_objs);
		
		// --------- Get FB and insert ----------
		printf("Inserting Row %d into Bucket %ld\n", rows_loaded_into_fb, oid);

		bucketPtr = GetAndInitializeBucket(FBmap,oid,nullbits,flxPtr);

		rows_loaded_into_fb++;
		delete nullbits;

		// ----------- Flush if rows_flush was met -----------
		if( bucketPtr->rowsv->size() >= flush_rows) {
			printf("\tFlushing bucket %ld to Ceph with %d rows\n", oid, bucketPtr->nrows);
			// Flush FlatBuffer to Ceph (currently writes to a file on disk)
			flushFlatBuffer(SKYHOOK_VERSION, SCHEMA_VERSION, bucketPtr, SCHEMA, num_objs);
			FBmap.erase(oid);
		}
		// Get next row
		parsedRow = getNextRow(inFile);
	}

// ------------- Iterate over map and flush each bucket --------------
	printf("\nFlush the remaining buckets in map\n");

	for (auto& x: FBmap) {
		bucket_t *b = x.second;
                printf("\tFlushing bucket %ld to Ceph with %d rows\n", b->oid, b->nrows);

		flushFlatBuffer(SKYHOOK_VERSION, SCHEMA_VERSION, b, SCHEMA, num_objs);
	}

	FBmap.clear();
        
        printf("Done flushing all the objects\n");
        
	// Close .csv file
	if( inFile.is_open() )
		inFile.close();

	return 0;
}

std::vector<std::string> split(const std::string &s, char delim) {
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
	cout<<"'"<<file_name<<"' was sucessfully opened!"<<endl<<endl;
}

uint32_t promptIntVariable(string variable, string num_string) {
	stringstream myStream(num_string);
	uint32_t n = 0;

	while( !(myStream >> n) ) {
		cout<<"How many "<<variable<<"?: ";
		getline(cin, num_string);
		stringstream newStream(num_string);
		if(newStream >> n)
			break;
		cout<<"Invalid number, please try again"<<endl;
	}
	cout<<"Number of "<<variable<<": "<<n<<endl<<endl;
	return n;
}

vector<string> getNextRow(ifstream& inFile) {
        // Get a row
        string row;
        getline(inFile, row);
        // Split by '|' deliminator
        return split(row, '|');
}

Tables::schema_vec getSchema(vector<int>& compositeKey, string& schema_file_name) {
	ifstream schemaFile;

	vector<int> schema;

	// Try to open schema file
	schemaFile.open(schema_file_name, std::ifstream::in);

	// If schema file didn't open, prompt for schema file name
	while(!schemaFile)
	{
		if(strcmp(schema_file_name.c_str(),"q")==0 || strcmp(schema_file_name.c_str(),"Q")==0 )
			exit(0);

		cout<<"Cannot open file! \t\t(Press Q or q to quit program)"<<endl;
		cout<<"Which file will we get the schema from? : ";
		getline(cin, schema_file_name);
		schemaFile.open(schema_file_name, ifstream::in);
	}


	// Parse Schema File for Composite Key
	string line = "";
	vector<string> splitLine;
	// Parse Schema File for Data Types
	while( getline(schemaFile,line) ) {
		splitLine = split(line, ' ');
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

	cout<<"'"<<schema_file_name<<"' was sucessfully read and schema vector passed back!"<<endl<<endl;	

	return sky_schema; 

}

vector<uint8_t> initializeFlexBuffer(vector<string> parsedRow, Tables::schema_vec schema,vector<uint64_t> *nullbits){
	flexbuffers::Builder *flx = new flexbuffers::Builder();
	getFlxBuffer(flx, parsedRow, schema, nullbits); // load parsed row into our flxBuilder and update nullbits
	vector<uint8_t> flxPtr = flx->GetBuffer();      // get pointer to FlexBuffer
	delete flx;
	return flxPtr;
}


void getFlxBuffer(flxBuilder *flx, vector<string> parsedRow, Tables::schema_vec schema, vector<uint64_t> *nullbits) {
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
					case Tables::SDT_INT8: {
                                                flx->Add(static_cast<int8_t>(0));
                                                break;
                                        }
					case Tables::SDT_INT16: {
                                                flx->Add(static_cast<int16_t>(0));
                                                break;
                                        }
					case Tables::SDT_INT32: {
                                                flx->Add(static_cast<int32_t>(0));
                                                break;
                                        }
                                        case Tables::SDT_INT64: {
                                                flx->Add(static_cast<int64_t>(0));
                                                break;
                                        }
					case Tables::SDT_UINT8: {
                                                flx->Add(static_cast<uint8_t>(0));
                                                break;
                                        }
					case Tables::SDT_UINT16: {
                                                flx->Add(static_cast<uint16_t>(0));
                                                break;
                                        }
					case Tables::SDT_UINT32: {
                                                flx->Add(static_cast<uint32_t>(0));
                                                break;
                                        }
                                        case Tables::SDT_UINT64: {
                                                flx->Add(static_cast<uint64_t>(0));
                                                break;
                                        }
					case Tables::SDT_CHAR: {
                                                flx->Add(static_cast<char>(0));
                                                break;
                                        }
 					case Tables::SDT_UCHAR: {
                                                flx->Add(static_cast<unsigned char>(0));
                                                break;
                                        }
					case Tables::SDT_BOOL: {
                                                flx->Add(false);
						break;
					}
					case Tables::SDT_FLOAT: {
                                                flx->Add(static_cast<float>(0));
						break;
					}
					case Tables::SDT_DOUBLE: {
                                                flx->Add(static_cast<double>(0));
						break;
					}
					case Tables::SDT_DATE: {
                                                flx->Add("0000-00-00");
						break;
					}
					case Tables::SDT_STRING: {
                                                flx->Add("This will be pooled with strings.");
						break;
					}
					default: {
                                                flx->Add("EMPTY");
                                                break;
                                        }
				}
			}
			else {
				switch(col.type) {
					case Tables::SDT_INT8: {
						flx->Add(static_cast<int8_t>(stoi(parsedRow[col.idx].c_str())));
						break;
					}
					case Tables::SDT_INT16: {
						flx->Add(static_cast<int16_t>(stoi(parsedRow[col.idx].c_str())));
						break;
					}
					case Tables::SDT_INT32: {
						flx->Add(static_cast<int32_t>(stoi(parsedRow[col.idx].c_str())));
						break;
					}
                                        case Tables::SDT_INT64:{
                                                flx->Add(static_cast<int64_t>(stoi(parsedRow[col.idx].c_str())));
						break;
                                        } 
					case Tables::SDT_UINT8: {
                                                flx->Add(static_cast<uint8_t>(stoul(parsedRow[col.idx].c_str())));
						break;
                                        }
					case Tables::SDT_UINT16: {
                                                flx->Add(static_cast<uint16_t>(stoul(parsedRow[col.idx].c_str())));
						break;
                                        }
					case Tables::SDT_UINT32: {
                                                flx->Add(static_cast<uint32_t>(stoul(parsedRow[col.idx].c_str())));
						break;
                                        }
                                        case Tables::SDT_UINT64: {
                                                flx->Add(static_cast<uint64_t>(stoul(parsedRow[col.idx].c_str())));
						break;
                                        }
 					case Tables::SDT_CHAR: {
                                                flx->Add(static_cast<char>(parsedRow[col.idx][0]));
						break;
					}
 					case Tables::SDT_UCHAR: {
                                                flx->Add(static_cast<unsigned char>(parsedRow[col.idx][0]));
						break;
					}
					case Tables::SDT_BOOL: {
                                                flx->Add(static_cast<bool>(parsedRow[col.idx].c_str()));
						break;
					}
					case Tables::SDT_FLOAT: {
                                                flx->Add(static_cast<float>(stof(parsedRow[col.idx].c_str())));
						break;
					}
					case Tables::SDT_DOUBLE: {
                                                flx->Add(static_cast<double>(stod(parsedRow[col.idx].c_str())));
						break;
					}
					case Tables::SDT_DATE: {
                                                flx->Add(parsedRow[col.idx].c_str());
						break;
					}
					case Tables::SDT_STRING: {
                                                flx->Add(parsedRow[col.idx].c_str());
						break;
					}
					default: {
                                                flx->Add("EMPTY");
                                                break;
                                        }
				}
			}
		}
	}); 
	flx->Finish();
}

uint64_t hashCompositeKey(vector<int> compositeKeyIndexes, vector<string> parsedRow) {
	// Hash the Composite Key
	uint64_t hashKey=0, upper=0, lower=0;
	stringstream(parsedRow[compositeKeyIndexes[0]]) >> upper;
	hashKey = upper << 32;
	if( compositeKeyIndexes.size() > 1) {
		stringstream(parsedRow[compositeKeyIndexes[1]]) >> lower;
		hashKey = hashKey | lower;
	}
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

bucket_t *GetAndInitializeBucket(map<uint64_t, bucket_t *> &FBmap,uint64_t oid,vector<uint64_t> *nullbits,vector<uint8_t> flxPtr){
	bucket_t *bucketPtr;
	bucketPtr = retrieveBucketFromOID(FBmap, oid);

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

bucket_t *retrieveBucketFromOID(map<uint64_t, bucket_t *> &FBmap, uint64_t oid) {
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
		bucketPtr->table_name = "LINEITEM";
		bucketPtr->fb = new fbBuilder();
		bucketPtr->deletev = new delete_vector();
		bucketPtr->rowsv = new rows_vector();
		FBmap[oid] = bucketPtr;
	}
	return bucketPtr;
}

void insertRowIntoBucket(fbb fbPtr, uint64_t RID, vector<uint64_t> *nullbits, vector<uint8_t> flxPtr, delete_vector *deletePtr, rows_vector *rowsPtr) {

        // Serialize FlexBuffer row into FlatBufferBuilder
        auto flxSerial = fbPtr->CreateVector(flxPtr);
        auto nullbitsSerial = fbPtr->CreateVector(*nullbits);
        auto rowOffset = CreateRecord(*fbPtr, RID, nullbitsSerial, flxSerial);
        deletePtr->push_back(0);
        rowsPtr->push_back(rowOffset);
}

uint64_t getNextRID() {
	return RID++;
}

void flushFlatBuffer(uint8_t skyhook_v, uint8_t schema_v, bucket_t *bucketPtr, string schema, uint64_t numOfObjs) {
        fbb fbPtr = bucketPtr->fb;
	delete_vector *deletePtr;
        rows_vector *rowsPtr;

	deletePtr = bucketPtr->deletev;
        rowsPtr = bucketPtr->rowsv;
	
	// Finish FlatBuffer
        finishFlatBuffer(fbPtr, skyhook_v, schema_v, bucketPtr->table_name, schema, deletePtr, rowsPtr, bucketPtr->nrows);
        uint64_t oid = bucketPtr->oid;
        // Flush to Ceph Here TO OID bucket with n Rows or Crash if Failed
        if(writeToDisk(oid, schema_v, bucketPtr, numOfObjs) < 0)
                exit(0);
        // Deallocate pointers
        deleteBucket(bucketPtr, fbPtr, deletePtr, rowsPtr);
}


/* TODO: set default version numbers to version fields (i.e. data_structure_type, fb_version, data_structure_version). */
void finishFlatBuffer(fbb fbPtr, uint8_t skyhook_v, uint8_t schema_v, string table_name, string schema, delete_vector *deletePtr, rows_vector *rowsPtr, uint32_t nrows) {
        auto data_format_type = skyhook_v;
        auto skyhook_version = 2; //skyhook_v
        auto data_structure_version = schema_v;
	auto data_schema_version = schema_v;	
        auto data_schema = fbPtr->CreateString(schema);                
        auto db_schema = fbPtr->CreateString("*");     
        auto table_n = fbPtr->CreateString(table_name);               
        auto delete_vector = fbPtr->CreateVector(*deletePtr);             
        auto rows = fbPtr->CreateVector(*rowsPtr);

        auto tableOffset = CreateTable(*fbPtr, data_format_type, skyhook_version, data_structure_version, data_schema_version, data_schema, db_schema, table_n, delete_vector, rows, nrows);

        fbPtr->Finish(tableOffset);
}


/* TODO: instead of writing objects to disk, write directly to ceph. */
int writeToDisk(uint64_t oid, uint8_t schema_v, bucket_t *bucketPtr, uint64_t numOfObjs) {

	string table_name = bucketPtr->table_name;
	fbb fbPtr = bucketPtr->fb;

        int buff_size = fbPtr->GetSize();
        const char *fb_ptr_char = reinterpret_cast<char*>(fbPtr->GetBufferPointer());
        bufferlist bl;
        bl.append(fb_ptr_char,buff_size);
        bufferlist wrapper_bl;
        ::encode(bl, wrapper_bl);
        int mode = 0600;
        string fname = "Skyhook.v2."+ table_name + "." + to_string(oid) + ".1-" + to_string(numOfObjs);
        wrapper_bl.write_file(fname.c_str(), mode);
	printf("buff size: %d, bl size: %d, wrapper_bl size: %d\n", buff_size, bl.length(), wrapper_bl.length());
	
        return 0;
}

void deleteBucket(bucket_t *bucketPtr, fbb fbPtr, delete_vector *deletePtr, rows_vector *rowsPtr) {
                printf("Clearing FB Ptr and RowsVector Ptr, Delete Bucket from Map\n\n");
                fbPtr->Reset();
                delete fbPtr;
                deletePtr->clear();
                delete deletePtr;
                rowsPtr->clear();
                delete rowsPtr;
                delete bucketPtr;
}
