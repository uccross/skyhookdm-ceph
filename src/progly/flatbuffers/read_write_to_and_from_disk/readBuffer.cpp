/*
* Copyright (C) 2018 The Regents of the University of California
* All Rights Reserved
*
* This library can redistribute it and/or modify under the terms
* of the GNU Lesser General Public License Version 2.1 as published
* by the Free Software Foundation.
*
*/

// This is still in progress

#include <fcntl.h> // system call open
#include <iostream>
#include <fstream>
#include <sstream>
#include <map>
#include <unistd.h>	// for getOpt
#include "../header_files/flatbuffers/flexbuffers.h"
#include "../header_files/skyhookv1_generated.h"

using namespace std;
using namespace Tables;

const uint8_t SKYHOOK_VERSION = 1;
const uint8_t SCHEMA_VERSION = 1;
uint64_t RID = 0;
enum DataType {TypeInt = 1, TypeDouble, TypeChar, TypeDate, TypeString};
typedef flatbuffers::FlatBufferBuilder fbBuilder;
typedef flatbuffers::FlatBufferBuilder* fbb;
typedef flexbuffers::Builder flxBuilder;
typedef vector<uint8_t> delete_vector;
typedef vector<flatbuffers::Offset<Row>> rows_vector;

typedef struct {
	uint64_t oid;
	uint32_t nrows;
	string table_name;
	fbb fb;
	delete_vector *deletev;
	rows_vector *rowsv;
} bucket_t;

std::vector<std::string> split(const std::string &s, char delim);
std::vector<int> splitDate(const std::string &s);
string int_to_binary(uint64_t);
void initializeNullbits(vector<uint64_t> *);
void helpMenu();
void promptDataFile(ifstream&, string&);
vector<int> getSchema(vector<string>&, string&);
uint32_t promptIntVariable(string, string);
uint64_t getNextRID();
bucket_t *retrieveBucketFromOID(map<uint64_t, bucket_t *> &, uint64_t);
void insertRowIntoBucket(fbb, uint64_t, vector<uint64_t> *, uint8_t, vector<uint8_t>, delete_vector *, rows_vector *);
void finishFlatBuffer(fbb, int8_t, string, delete_vector *, rows_vector *, uint32_t);
void deleteBucket(bucket_t *, map<uint64_t, bucket_t *> &, uint64_t, fbb, delete_vector *, rows_vector *);
int writeToDisk(uint64_t, fbb);
void flushFlatBuffer(map<uint64_t, bucket_t *> &, fbb, uint8_t, bucket_t *, delete_vector *, rows_vector *);

vector<string> getNextRow(ifstream& inFile);
void getFlxBuffer(flexbuffers::Builder *, vector<string>, vector<int>, vector<uint64_t> *);
int findKeyIndexWithinSchema(string, string);
uint64_t hashCompositeKey(string, vector<string>, vector<string>);
uint64_t jumpConsistentHash(uint64_t, uint64_t);

int main()
{
	struct stat stat_buf;
	string filename = "obj.3.bin";
	int rc = stat(filename.c_str(), &stat_buf);
	int size = rc==0?stat_buf.st_size:-1;
	cout<<"size: "<<size<<endl;	

	int file = 0;
	if( (file = open("3.bin",O_RDONLY)) < -1)
		return -1;

	// Get root pointer
	uint8_t offset[4];
	int current_offset = 0;

	if(read(file,reinterpret_cast<char *>(&offset), 4) != 4)
		return -1;
	int rootPtr = (int)offset[0];

	// Get vTable Offset
	lseek(file,rootPtr,SEEK_SET);
	if(read(file,reinterpret_cast<char *>(&offset), 4) != 4)
		return -1;
	int vtable = rootPtr - (int)offset[0];

	// Get Data (rows_vector) Field Offset from Root FB vtable
	int offset_to_rows_vec = 10;
	lseek(file,vtable + offset_to_rows_vec, SEEK_SET);
	if(read(file,reinterpret_cast<char *>(&offset), 4) != 4)
		return -1;
	current_offset = rootPtr + (int)offset[0];

	// Get Offset to Rows Vector
	lseek(file,current_offset,SEEK_SET);
	if(read(file,reinterpret_cast<char *>(&offset), 4) != 4)
		return -1;
	current_offset += (int)offset[0];	

	// Get Vector Size  within vector
	lseek(file, current_offset, SEEK_SET);
	if(read(file,reinterpret_cast<char *>(&offset), 4) != 4)
		return -1;
	int numRows = (int)offset[0];


	vector<uint32_t> row_offsets;
	// For each Row, get the offset to Row FB (Offsets to Row FB are 32 bit values)
	for(int i=0;i<numRows;i++) {
		offset[0] = offset[1] = offset[2] = offset[3] = 0;
		current_offset += 4;
		lseek(file, current_offset, SEEK_SET);
		if(read(file,reinterpret_cast<char *>(&offset), 4) != 4)
			return -1;
		uint32_t value = (int)offset[0] + ((int)offset[1] << 8) + ((int)offset[2] << 16) + ((int)offset[3] << 24);

		cout<<"value of "<<i<<" is "<<value<<endl;
		int row_offset = current_offset + value;
		cout<<"row is at "<<row_offset<<endl;
	
		lseek(file,row_offset,SEEK_SET);	
		if(read(file,reinterpret_cast<char *>(&offset), 4) != 4)
			return -1;
		cout<<"POINTER #"<<(int)offset[0]<<" "<<(int)offset[1]<<" "<<(int)offset[2]<<" "<<(int)offset[3]<<endl;
		uint32_t value2 = (int)offset[0] + ((int)offset[1] << 8) + ((int)offset[2] << 16) + ((int)offset[3] << 24);
		int32_t vtable_offset2 = (int32_t) value2;
		cout<<"offset from "<<row_offset<<": "<<vtable_offset2<<"= "<<row_offset-vtable_offset2<<endl;

		cout<<endl;
	}

		

//	lseek(file,468,SEEK_SET);
//	uint8_t row2[251];
//	if(read(file,reinterpret_cast<char*>(&row2),251) != 251)
//		return -1;
//	vector<uint8_t> p;
//	for(int i=0;i<251;i++) {
//		p.push_back(row2[i]);
//	}
//	auto row = flexbuffers::GetRoot(p).AsVector();
//	cout<<row[2].AsUInt32()<<endl;

	const int file_size = 1864;
	lseek(file,0,SEEK_SET);
	uint8_t bytes[file_size];
	if(read(file,reinterpret_cast<char *>(&bytes),file_size) !=file_size)
		return -1;
	for(int i=0;i<file_size;i++) {
//		if(bytes[i] == 84 || bytes[i] == 65 || bytes[i] == 75)
//		cout<<"\t\t\t";
		if(bytes[i] == 97 || bytes[i] == 98 || bytes[i] == 111 || bytes[i] == 118)
			cout<<"\t\t\t";
		int diff = i+bytes[i];
		if(diff ==  784|| diff == 464 || diff == 156)
			cout<<"\t\t\t\t";
		printf("Byte %d: %u\n",i,bytes[i]);
	}


	lseek(file,0,SEEK_SET);
	rc = stat(filename.c_str(), &stat_buf);
	size = rc==0?stat_buf.st_size:-1;
	cout<<"size: "<<size<<endl;	

	uint8_t contents[1864];
	if(read(file,reinterpret_cast<char *>(&contents), size) !=size)
		return -1;
	auto table = GetTable(contents);
	const flatbuffers::Vector<flatbuffers::Offset<Row>>* recs = table->rows();
	for(int i=0;i<6;i++) {
		auto flxRoot = recs->Get(i)->data_flexbuffer_root();
		auto orderkey = flxRoot.AsVector()[15].AsString();
		cout<<"one of them: "<<orderkey.str()<<endl;
	}
/*
// ----------------------------------------Verify Configurable Variables or Prompt For Them------------------------
	int opt;
	while( (opt = getopt(argc, argv, "hf:s:o:r:n:")) != -1) {
		switch(opt) {
			case 'f':
				// Open .csv file
				file_name = optarg;
				promptDataFile(inFile, file_name);
				break;
			case 's':
				// Get Schema (vector of enum types), schema file name, composite key
				schema_file_name = optarg;
				schema = getSchema(composite_key, schema_file_name);	// returns vector of enum data types and composite keys
				break;
			case 'o':
				// Set # of Objects
				num_objs = promptIntVariable("objects", optarg);
				num_buckets = num_objs;
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
		}
	}
*/
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

std::vector<int> splitDate(const std::string &s) {
	std::istringstream ss(s);
	std::string item;
	std::vector<int> tokens;
	while( getline(ss, item, '-')) {
		tokens.push_back(atoi(item.c_str()));
	}
	return tokens;
}

void helpMenu() {
	printf("-h HELP MENU\n");
	printf("\t-f [file_name]\n");
	printf("\t-s [schema_file_name]\n");
	printf("\t-o [number_of_objects]\n");
	printf("\t-r [number_of_rows_until_flush]\n");
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

vector<int> getSchema(vector<string>& compositeKey, string& schema_file_name) {
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
	getline(schemaFile, line);
	compositeKey = split(line,' ');

	// Parse Schema File for Data Types
	while( getline(schemaFile, line) ) {
		vector<string> parsedData = split(line, ' ');
		schema.push_back( atoi(parsedData[3].c_str()) );
	}

	cout<<"'"<<schema_file_name<<"' was sucessfully read and schema vector passed back!"<<endl<<endl;
	schemaFile.close();
	
	return schema;
}

vector<string> getNextRow(ifstream& inFile) {
	// Get a row
	string row;
	getline(inFile, row);
	// Split by '|' deliminator
	return split(row, '|');
}

void getFlxBuffer(flxBuilder *flx, vector<string> parsedRow, vector<int> schema, vector<uint64_t> *nullbits) {
	bool nullFlag = false;
	string nullcmp = "NULL";
	// Create Flexbuffer from Parsed Row and Schema
	flx->Vector([&]() {
		for(int i=0;i<(int)schema.size();i++) {
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
				// Put a dummy variable to hold the index for future updates
				switch(schema[i]) {
					case TypeInt:
						flx->Int(0);
						break;
					case TypeDouble:
						flx->Double(0.0);
						break;
					case TypeChar:
						flx->Int('0');
						break;
					case TypeDate:
						flx->String("1993-11-10");
						break;
					case TypeString:
						flx->String("This will be pooled with strings");
						break;
					default:
						flx->String("EMPTY");
						break;
				}
			}
			else {
				switch(schema[i]) {
					case TypeInt: {
						flx->Int( atoi(parsedRow[i].c_str()) );
						break;
					}
					case TypeDouble: {
						flx->Double( stod(parsedRow[i]) );
						break;
					}
					case TypeChar: {
						flx->Int( parsedRow[i][0] );
						break;
					}
					case TypeDate: {
						flx->String( parsedRow[i]);
						break;
					}
					case TypeString: {
						flx->String( parsedRow[i]);
						break;
					}
					default: {
						flx->String("EMPTY");
						break;
					}
				}
			}
		}
	});
	flx->Finish();
}

int findKeyIndexWithinSchema(string schemaFileName, string key) {
	ifstream schemaFile;
	schemaFile.open(schemaFileName, ifstream::in);
	string line;
	getline(schemaFile, line); // skip composite keys header

	while( getline(schemaFile, line) ) {
		vector<string> parsedData = split(line, ' ');
		// Hardcoded to know schema file 
		//	has 'name' field as 2nd column of schema file
		//	and has 'field number' as 1st column of schema file
		//	field number | name | data type | data type enum
		if( strcmp(parsedData[1].c_str(), key.c_str()) == 0) {
			schemaFile.close();
			return atoi(parsedData[0].c_str());
		}
	}
	// Else key was not found in schema
	schemaFile.close();
	return -1;
}

uint64_t hashCompositeKey(string schemaFileName, vector<string> compositeKey, vector<string> parsedRow) {
	// Get the Index of Key/s from SchemaFile
	vector<int> keyIndexes;
	for(int i=0; i<(int)compositeKey.size();i++) {
		keyIndexes.push_back( findKeyIndexWithinSchema(schemaFileName, compositeKey[i]) );
	}
	// Hash the Composite Key
	uint64_t hashKey=0, upper=0, lower=0;
	stringstream(parsedRow[keyIndexes[0]]) >> upper;
	hashKey = upper << 32;
	if( keyIndexes.size() > 1) {
		stringstream(parsedRow[keyIndexes[1]]) >> lower;
		hashKey = hashKey | lower;
	}
	return hashKey;
}

uint64_t jumpConsistentHash(uint64_t key, uint64_t num_buckets) {
	// Source:
	// A Fast, Minimal Memory, Consistent Hash Algorithm
	// https://arxiv.org/ftp/arxiv/papers/1406/1406.2294.pdf
	//
	int64_t b=-1l, j=0l;
	while(j < (int64_t)num_buckets) {
		b = j;
		key = key * 286293355588894185ULL + 1;
		j = (b+1) * (double(1LL << 31) / double((key>>33) + 1));
	}
	return b;
}

string int_to_binary(uint64_t value) {
	string output = "";
	for(int i=63;i>=0;i--)
	{
		if( value & ( 1lu<<i) )
			output += "1";
		else
			output += "0";
	}
	return output;
}

void initializeNullbits(vector<uint64_t> *nullbits) {
	nullbits->push_back((uint64_t) 0);
	nullbits->push_back((uint64_t) 0);
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
		FBmap.insert(pair<uint64_t, bucket_t *>(oid,bucketPtr));
	}
	return bucketPtr;
}

void finishFlatBuffer(fbb fbPtr, int8_t version, string table_name, delete_vector *deletePtr, rows_vector *rowsPtr, uint32_t nrows) {
	auto table_nameOffset = fbPtr->CreateString(table_name);
	auto delete_vecOffset = fbPtr->CreateVector(*deletePtr);
	auto rows_vecOffset = fbPtr->CreateVector(*rowsPtr);
	auto tableOffset = CreateTable(*fbPtr, version, table_nameOffset, delete_vecOffset, rows_vecOffset, nrows);
	fbPtr->Finish(tableOffset);
}

void insertRowIntoBucket(fbb fbPtr, uint64_t RID, vector<uint64_t> *nullbits, uint8_t schema_version, vector<uint8_t> flxPtr, delete_vector *deletePtr, rows_vector *rowsPtr) {

	// Serialize FlexBuffer row into FlatBufferBuilder
	auto flxSerial = fbPtr->CreateVector(flxPtr);
	auto nullbitsSerial = fbPtr->CreateVector(*nullbits);
	auto rowOffset = CreateRow(*fbPtr, RID, nullbitsSerial, schema_version, flxSerial);
	deletePtr->push_back(0);
	rowsPtr->push_back(rowOffset);
}

void deleteBucket(bucket_t *bucketPtr, map<uint64_t, bucket_t *> &FBmap, uint64_t oid, fbb fbPtr, delete_vector *deletePtr, rows_vector *rowsPtr) {
		printf("Clearing FB Ptr and RowsVector Ptr, Delete Bucket from Map\n\n");
		
		FBmap.erase(oid);
		fbPtr->Reset();
		delete fbPtr;
		deletePtr->clear();
		delete deletePtr;
		rowsPtr->clear();
		delete rowsPtr;
		delete bucketPtr;
}

uint64_t getNextRID() {
	return RID++;
}

int writeToDisk(uint64_t oid, fbb fbPtr) {
	
	string file_name = to_string(oid) + ".bin";
	int fd = open(file_name.c_str(), O_WRONLY | O_CREAT | O_APPEND, S_IRUSR | S_IWUSR);
	if( fd < 0 ) {
		printf("Error opening '\%s'!\n", file_name.c_str());
		return -1;
	}

	struct stat stat_buf;
	int rc = stat(file_name.c_str(), &stat_buf);
	int old_size = rc==0? stat_buf.st_size:-1;

	int buff_size = fbPtr->GetSize();
	int bytes_written = write(fd, (void*) fbPtr->GetBufferPointer(), (size_t) buff_size );

	if( bytes_written != buff_size) {
		printf("Error writing bytes to disk with oid: %ld!\n", oid);
		return -1;
	}

	rc = stat(file_name.c_str(), &stat_buf);
	int new_size = rc==0? stat_buf.st_size:-1;

	// Assert that entire buffer was written
	assert(new_size - old_size == buff_size);

	printf("Wrote %d bytes to file '%s' ----- New file size: %d\n", bytes_written, file_name.c_str(), new_size);
	close(fd);
	return 0;
}

void flushFlatBuffer(map<uint64_t, bucket_t *> &FBmap, fbb fbPtr, uint8_t SKYHOOK_VERSION, bucket_t *bucketPtr, delete_vector *deletePtr, rows_vector *rowsPtr) {
	// Finish FlatBuffer
	finishFlatBuffer(fbPtr, SKYHOOK_VERSION, bucketPtr->table_name, deletePtr, rowsPtr, bucketPtr->nrows);

	uint64_t oid = bucketPtr->oid;
	// Flush to Ceph Here TO OID bucket with n Rows or Crash if Failed
	if(writeToDisk(oid, fbPtr) < 0)
		exit(0);
	// Remove bucket from FBMap and Deallocate pointers
	deleteBucket(bucketPtr, FBmap, oid, fbPtr, deletePtr, rowsPtr);
}
