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
// Currently assumes one flatbuffer per object file

#include <fcntl.h> // system call open
#include <iostream>
#include <fstream>
#include <sstream>
#include <algorithm> // for string reversal
#include <unistd.h>	// for getOpt
#include "../header_files/flatbuffers/flexbuffers.h"
#include "../header_files/skyhookv1_generated.h"

using namespace std;
using namespace Tables;

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

enum DataType {TypeInt = 1, TypeDouble, TypeChar, TypeDate, TypeString};

typedef flatbuffers::FlatBufferBuilder fbBuilder;
typedef flatbuffers::FlatBufferBuilder* fbb;
typedef flexbuffers::Builder flxBuilder;
typedef vector<uint8_t> delete_vector;
typedef vector<flatbuffers::Offset<Row>> rows_vector;

typedef struct {
	int skyhook_version;
	int schema_version;
	string table_name;
	string schema;
	delete_vector delete_vec;
	int rows_offset;
	int nrows;
} root_header;

typedef struct {
	int64_t RID;
	vector<uint64_t> nullbits;
	int data_offset;	
} row_header;

typedef struct {
	int orderkey;
	int partkey;
	int suppkey;
	int linenumber;
	float quantity;
	float extendedprice;
	float discount;
	float tax;
	int8_t returnflag;
	int8_t linestatus;
	string shipdate;
	string receiptdate;
	string commitdate;
	string shipinstruct;
	string shipmode;
	string comment;
} lineitem;

vector<string> split(const string &, char);
void helpMenu();
void promptDataFile(string&, int&);
string getReturnSchema(vector<int> &, vector<int> &, string&);
uint32_t promptIntVariable(string, string);

void readOffset(int, int, uint8_t *);
void readRootHeader(int, root_header *);
void readRowOffsets(int, int, vector<int> &);
void readRowHeader(int, int, row_header *);
void readData(int, int, vector<uint8_t> &);

void find_vtable_given_row_ptr(int fd, int row_ptr);
void printBuffer(int, int);
void printRootHeader(root_header *);
void rootHeaderAssertion(const Tables::Table *, root_header *);
void rowHeaderAssertion(const Tables::Table *, int, row_header *);
void lineitemAssertion(const Tables::Table *, int, vector<uint8_t> &);

int main(int argc, char *argv[])
{
	int fd;
	string file_name = "";
	string schema_file_name = "";
	vector<int> returnSchema, returnTypes;
	string returnSchemaString = "";
// ----------------------------------------Verify Configurable Variables or Prompt For Them------------------------
	int opt;
	while( (opt = getopt(argc, argv, "hf:s:")) != -1) {
		switch(opt) {
			case 'f':
				// Open object file
				file_name = optarg;
				promptDataFile(file_name, fd);
				break;
			case 's':
				// Get Schema (vector of enum types), schema file name, composite key
				schema_file_name = optarg;
				returnSchemaString = getReturnSchema(returnSchema, returnTypes, schema_file_name);	// gets output schema and types
				break;
			case 'h':
				helpMenu();
				exit(0);
				break;
		}
	}
	
	// Get object file size
	struct stat stat_buf;
	int rc = stat(file_name.c_str(), &stat_buf);
	int size = rc==0 ? stat_buf.st_size : -1;
	cout<<file_name<<"'s size: "<<size<<endl<<endl;

// ----------------------------------- Get FlatBuffer using Object API ----------------------------------------------
	const int file_size = size;
	lseek(fd,0,SEEK_SET);
	uint8_t *contents = new uint8_t[file_size];
	if(read(fd,reinterpret_cast<char *>(contents), file_size) != file_size)
		return -1;
	auto table = GetTable(contents);

// -------------------------------------- Get data using our API ----------------------------------------------------

	// Get ROOT_TABLE Header
	root_header *root = new root_header();
	readRootHeader(fd, root);
	printRootHeader(root);
	rootHeaderAssertion(table, root);	// compares our header with object APIs header
	cout<<endl;
	
	// Get ROW_TABLE Offsets
	vector<int> row_offsets;
	readRowOffsets(fd, root->rows_offset, row_offsets);
	
	// Get ROW_TABLE Headers
	vector<row_header *>rows;
	for(int i=0;i<root->nrows;i++) {
		row_header *row = new row_header();
		readRowHeader(fd, row_offsets[i], row);
		rows.push_back(row);

		rowHeaderAssertion(table, i, row);	// compares row 'i's header with object API row header
	}
	cout<<endl;

	// Get Data FlexBuffer for each row and check with Flatbuffers API
	for(int rowNum=0;rowNum<(int)rows.size();rowNum++) {
		vector<uint8_t> data;
		readData(fd, rows[rowNum]->data_offset, data);
		lineitemAssertion(table, rowNum, data);		// compare data flexbuffer with object API
	}
	cout<<endl;

	// Build return flatbuffer
	fbBuilder fb(1024);
	delete_vector dead_vector;
	rows_vector row_vector;
	cout<<"Return Schema String:"<<endl;
	cout<<returnSchemaString<<endl<<endl;
	cout<<"Here are the rows returned:"<<endl;
	cout<<"\tSome values may be null, check nullbit vector first!"<<endl;
	cout<<"\tDefault null values were encoded in getFlexBuffer(..) function in writeBuffer.cpp"<<endl;
	for(int i = 0; i < (int)rows.size(); i++) {
		// Get FlexBuffer Data
		vector<uint8_t> data;
		readData(fd, rows[i]->data_offset, data);
		auto flxRow = flexbuffers::GetRoot(data).AsVector();
		
		flexbuffers::Builder *flx = new flexbuffers::Builder();
		flx->Vector([&]() {
			// returnSchema maps to column # in original schema
			// returnType maps to enum data type
			for(int j=0;j<(int)returnSchema.size();j++) {

				if(returnSchema[j] < 0 ) {
					// A negative column # means not in original schema
					// Do some processing (SUM, COUNT, etc)
				}
				else {
					switch(returnTypes[j]) {
						case TypeInt: {
							int value = flxRow[returnSchema[j]].AsInt32();
							flx->Int(value);
							cout<<value;
							break;
						}
						case TypeDouble: {
							double value = flxRow[returnSchema[j]].AsDouble();
							flx->Double(value);
							cout<<value;
							break;
						}
						case TypeChar: {
							int8_t value = flxRow[returnSchema[j]].AsInt8();
							flx->Int(value);
							cout<<(char)value;
							break;
						}
						case TypeDate: {
							string line = flxRow[returnSchema[j]].AsString().str();
							flx->String(line);
							cout<<line;
							break;
						}
						case TypeString: {
							string line = flxRow[returnSchema[j]].AsString().str();
							flx->String(line);
							cout<<line;
							break;
						}
						default: {
							// This is for other enum types for aggregations, e.g. SUM, MAX, MIN....
							flx->String("EMPTY");
							break;
						}
					}
				}
				// Display a '|' to separate rows
				if( j != (int)returnSchema.size() - 1 )
					cout<<"|";
				if( j == (int)returnSchema.size() -1 )
					cout<<endl;
			}
		});
		flx->Finish();
		auto flx_serial = fb.CreateVector(flx->GetBuffer());
		auto flx_nullbits =  fb.CreateVector(rows[i]->nullbits);
		// Create Row with current flexbuffer row
		auto rowOffset = CreateRow(fb, rows[i]->RID, flx_nullbits, flx_serial);
		// Continue building dead vector and rowOffsets vector
		dead_vector.push_back('0');
		row_vector.push_back(rowOffset);
		delete flx;
	}
	auto table_nameOffset = fb.CreateString(root->table_name);
	auto schema_Offset = fb.CreateString(returnSchemaString);
	auto delete_vecOffset = fb.CreateVector(dead_vector);
	auto rows_vecOffset = fb.CreateVector(row_vector);
	auto tableOffset = CreateTable(fb, root->skyhook_version, root->schema_version, table_nameOffset, schema_Offset, delete_vecOffset, rows_vecOffset, root->nrows);
	fb.Finish(tableOffset);
	printf("\nReturn FlatBuffer has been finished!\n");

	// Used for displaying and debugging entire FlatBuffer
	// printBuffer(fd, size);	

	// Free all memory that was allocated
	delete [] contents;
	delete root;
	for(int i=0;i<root->nrows;i++)
		delete rows[i];

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
}

void promptDataFile(string& file_name, int &fd) {
	ifstream inFile;
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
	inFile.close();
	fd = open(file_name.c_str(), O_RDONLY);
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

string getReturnSchema(vector<int> &returnSchema, vector<int> &returnTypes, string& schema_file_name) {
	ifstream schemaFile;
	string returnSchemaString = "";	

	// Try to open schema file
	schemaFile.open(schema_file_name, std::ifstream::in);

	// If schema file didn't open, prompt for schema file name
	while(!schemaFile)
	{
		if(strcmp(schema_file_name.c_str(),"q")==0 || strcmp(schema_file_name.c_str(),"Q")==0 )
			exit(0);

		cout<<"Cannot open file! \t\t(Press Q or q to quit program)"<<endl;
		cout<<"Which file will we get the return schema from? : ";
		getline(cin, schema_file_name);
		schemaFile.open(schema_file_name, ifstream::in);
	}
	// Parse Schema File for Composite Key
	string line = "";
	getline(schemaFile, line);
	// Unused compositeKey
	vector<string> compositeKey;
	compositeKey = split(line,' ');

	// Parse Schema File for Data Types
	while( getline(schemaFile, line) ) {
		// Get return schema and return type
		vector<string> parsedData = split(line, ' ');
		returnSchema.push_back( atoi(parsedData[0].c_str()) );
		returnTypes.push_back( atoi(parsedData[1].c_str()) );
		
		// Build schema string for return
		returnSchemaString += parsedData[0] + " " + parsedData[1] + "|";
	}

	cout<<"'"<<schema_file_name<<"' was sucessfully read and schema vector passed back!"<<endl<<endl;
	schemaFile.close();
	
	return returnSchemaString;
}

void readOffset(int fd, int current_offset, uint8_t *offset) {
	lseek(fd, current_offset , SEEK_SET);
	if(read(fd,reinterpret_cast<uint8_t *>(offset), sizeof(int)) != sizeof(int))
		exit(-1);
}


void readRootHeader(int fd, root_header *root) {
	uint8_t offset[4];
	int current_offset = 0;

	// Get root pointer of ROOT_TABLE ----------------------------------------------
	readOffset(fd, current_offset, offset);
	int rootPtr = (int)offset[0];

	// Get vTable pointer for ROOT_TABLE -------------------------------------------
	readOffset(fd, rootPtr, offset);
	int vtablePtr = rootPtr - (int)offset[0];
	
	// Get skyhook_version ---------------------------------------------------------
	readOffset(fd, vtablePtr+offset_to_skyhook_version, offset);
	current_offset = rootPtr + (int)offset[0];
	readOffset(fd, current_offset, offset);
	root->skyhook_version = (int)offset[0];

	// Get schema_version ----------------------------------------------------------
	readOffset(fd, vtablePtr+offset_to_schema_version, offset);
	current_offset = rootPtr + (int)offset[0];
	readOffset(fd, current_offset, offset);
	root->schema_version = (int)offset[0];

	// Get table_name --------------------------------------------------------------
	readOffset(fd, vtablePtr+offset_to_table_name, offset);
	current_offset = rootPtr + (int)offset[0];
	readOffset(fd, current_offset, offset);
	current_offset += (int)offset[0];
	
		// Read size of table_name string
	readOffset(fd, current_offset, offset);
	const int table_name_size = (int)offset[0];
		// Read table_name string
	current_offset += 4;
	lseek(fd,current_offset,SEEK_SET);
	char table_name[table_name_size];
	if(read(fd, reinterpret_cast<char *>(&table_name), table_name_size) != table_name_size)
		exit(-1);
	string t(table_name,table_name_size);
	root->table_name = t;

	// Get schema ------------------------------------------------------------------
	readOffset(fd, vtablePtr+offset_to_schema, offset);
	current_offset = rootPtr + (int)offset[0];
	readOffset(fd, current_offset, offset);
	current_offset += (int)offset[0];
		// Read size of schema string
	readOffset(fd, current_offset, offset);
	const int schema_size = (int)offset[0];
		// Read schema string
	current_offset += 4;
	lseek(fd,current_offset,SEEK_SET);
	char schema[schema_size];
	if(read(fd, reinterpret_cast<char *>(&schema), schema_size) != schema_size)
		exit(-1);
	string s(schema, schema_size);
	root->schema = s;

	// Get delete_vector ------------------------------------------------------------
	readOffset(fd, vtablePtr+offset_to_delete_vec, offset);
	current_offset = rootPtr + (int)offset[0];

	readOffset(fd, current_offset, offset);
	current_offset += (int)offset[0];

		// Read size of delete_vector
	readOffset(fd, current_offset, offset);
	const int delete_vec_size = (int)offset[0];
		// Read delete_vector
	current_offset += 4;
	lseek(fd, current_offset, SEEK_SET);	
	uint8_t delete_vec[delete_vec_size];
	if(read(fd, reinterpret_cast<char *>(&delete_vec), delete_vec_size) != delete_vec_size)
		exit(-1);
	for(int i=0;i<delete_vec_size;i++)
		root->delete_vec.push_back(delete_vec[i]);
	
	// Get rows offsets ---------------------------------------------------------------
	readOffset(fd, vtablePtr+offset_to_rows_vec, offset);
	current_offset = rootPtr + (int)offset[0];
	readOffset(fd, current_offset, offset);
	root->rows_offset = current_offset + (int)offset[0];

	// Get nrows -----------------------------------------------------------------------
	readOffset(fd, vtablePtr+offset_to_nrows, offset);
	current_offset = rootPtr + (int)offset[0];
	readOffset(fd, current_offset, offset);
	root->nrows = (int)offset[0];
}

void readRowHeader(int fd, int row_table_ptr, row_header *row) {
	uint8_t offset[4];
	int current_offset = row_table_ptr;
	
	// Get ROW_TABLE's vTable Offset ----------------------------------------------------
	readOffset(fd, current_offset, offset);
	int vtable_offset= (int)offset[0] + ((int)offset[1] << 8) + ((int)offset[2] << 16) + ((int)offset[3] << 24);
	int row_vtable_ptr = row_table_ptr - vtable_offset;
	
	// Get RID --------------------------------------------------------------------------
	readOffset(fd, row_vtable_ptr + offset_to_RID, offset);
	current_offset = row_table_ptr + (int)offset[0];
	lseek(fd, current_offset, SEEK_SET);
	if( read(fd, reinterpret_cast<uint8_t *>(&offset), sizeof(double)) != sizeof(double))
		exit(-1);
	row->RID = (int64_t)offset[0];
	
	// Get nullbits ---------------------------------------------------------------------
	readOffset(fd, row_vtable_ptr + offset_to_nullbits_vec, offset);
	current_offset = row_table_ptr + (int)offset[0];
		// Get offset to nullbits vector
	readOffset(fd, current_offset, offset);
	current_offset += (int)offset[0];
		// Get size of nullbits vector
	readOffset(fd, current_offset, offset);
	const int nullbits_size = (int)offset[0];
		// Copy nullbits
	current_offset += 4;
	lseek(fd,current_offset,SEEK_SET);
	uint8_t nullbits[nullbits_size*sizeof(double)];
	if(read(fd, reinterpret_cast<char *>(&nullbits), nullbits_size*sizeof(double)) != (int)(nullbits_size*sizeof(double)))
		exit(-1);
		// Building the two uint64_t values for nullbits vector
		// Done by manually shifting each bit and then summing them
		// Swaps endianness (or it reads little endian into a 64-bit int value)
	uint64_t null0 = (uint64_t)nullbits[0] + ((uint64_t)nullbits[1]<<8) + ((uint64_t)nullbits[2]<<16) + ((uint64_t)nullbits[3]<<24) + ((uint64_t)nullbits[4]<<32) +
				 ((uint64_t)nullbits[5]<<40) + ((uint64_t)nullbits[6]<<48) + ((uint64_t)nullbits[7]<<56);
	uint64_t null1 = (uint64_t)nullbits[8] + ((uint64_t)nullbits[9]<<8) + ((uint64_t)nullbits[10]<<16) + ((uint64_t)nullbits[11]<<24) + ((uint64_t)nullbits[12]<<32) +
				 ((uint64_t)nullbits[13]<<40) + ((uint64_t)nullbits[14]<<48) + ((uint64_t)nullbits[15]<<56);
	row->nullbits.push_back(null0);
	row->nullbits.push_back(null1);

	// Get data offset ------------------------------------------------------------------
	readOffset(fd, row_vtable_ptr + offset_to_data, offset);
	current_offset = row_table_ptr + (int)offset[0];
		// Get offset to data vector
	readOffset(fd, current_offset, offset);
	row->data_offset = current_offset + (int)offset[0];

}

void readRowOffsets(int fd, int rows_offset, vector<int> &row_offsets) {	
	uint8_t offset[4];
	int current_offset = rows_offset;

	// lseek to rows_vec starting point and get size of rows_vec
	readOffset(fd, current_offset, offset);
	int nrows = (int)offset[0];
	for(int i=0;i<nrows;i++) {
		current_offset += 4;
		// Get ROW_TABLE Offset

		readOffset(fd, current_offset, offset);
		int row_root_ptr = (int)offset[0] + ((int)offset[1] << 8) + ((int)offset[2] << 16) + ((int)offset[3] << 24);
		int row_ptr = current_offset + row_root_ptr;
		row_offsets.push_back(row_ptr);
	}
}

void find_vtable_given_row_ptr(int fd, int row_ptr) {
	uint8_t offset[4];
	lseek(fd, row_ptr, SEEK_SET);
	if( read(fd, reinterpret_cast<char *>(&offset), 4) != 4)
		exit(0);
	
	int p = (int)offset[0] + ((int)offset[1] << 8) + ((int)offset[2] << 16) + ((int)offset[3] << 24);
	int row_v = row_ptr - p;
	cout<<"row's vtable is at: "<<row_v<<endl;
}

void printBuffer(int fd, int size) {
	const int file_size = size;
	lseek(fd,0,SEEK_SET);
	uint8_t bytes[file_size];

	if(read(fd,reinterpret_cast<char *>(&bytes), file_size) != file_size)
		exit(-1);

	for(int i=0;i<file_size;i++) {
		printf("Byte %d: %u\n",i,bytes[i]);
	}
}

void readData(int fd, int data_offset, vector<uint8_t> &data) {
	uint8_t offset[4];
	int current_offset = data_offset;

	readOffset(fd, data_offset, offset);

	const int data_size = (int)offset[0];

	current_offset += 4;
	lseek(fd, current_offset, SEEK_SET);
	uint8_t buffer[data_size];
	if(read(fd, reinterpret_cast<char *>(&buffer), data_size) != data_size)
		exit(-1);
	for(int i=0;i<data_size;i++)
		data.push_back(buffer[i]);
}

void printRootHeader(root_header *root) {
	cout<<"[ROOT HEADER]"<<endl;
	cout<<"skyhook version: "<<root->skyhook_version<<endl;
	cout<<"schema version: "<<root->schema_version<<endl;
	cout<<"table name: "<<root->table_name<<endl;
	cout<<"schema: "<<root->schema<<endl;
	cout<<"delete vector: [";
	for(int i=0;i< (int)root->delete_vec.size();i++) {
		cout<<(int)root->delete_vec[i];
		if(i != (int)root->delete_vec.size()-1 )
			cout<<", ";
	}
	cout<<"]"<<endl;
	cout<<"row offset: "<<root->rows_offset<<endl;
	cout<<"nrows: "<<root->nrows<<endl;
	cout<<endl;
}

void rootHeaderAssertion(const Tables::Table *table, root_header *root) {
	// Get API root_header
	root_header *api_root = new root_header();
	api_root->skyhook_version = table->skyhook_version();
	api_root->schema_version = table->schema_version();
	api_root->table_name = table->table_name()->str();
	api_root->schema = table->schema()->str();
	auto root_delete_vec = table->delete_vector();
	for(int i=0;i<(int)root_delete_vec->size();i++)
		api_root->delete_vec.push_back(root_delete_vec->Get(i));
	api_root->nrows = table->nrows();

	// Check that API root_header is the same as our root_header
	assert(api_root->skyhook_version == root->skyhook_version);
	assert(api_root->schema_version == root->schema_version);
	assert(strcmp(api_root->table_name.c_str(), root->table_name.c_str()) == 0);
	assert(strcmp(api_root->schema.c_str(), root->schema.c_str()) == 0);
	for(int i=0; i<(int)api_root->delete_vec.size(); i++)
		assert(api_root->delete_vec[i] == root->delete_vec[i]);
	assert(api_root->nrows == root->nrows);
	
	delete api_root;
	printf("Root Header Assertion Passed!\n");
}

void rowHeaderAssertion(const Tables::Table *table, int rowNum, row_header *row) {
	// Get API row_header
	const flatbuffers::Vector<flatbuffers::Offset<Row>>* recs = table->rows();
	auto api_row = recs->Get(rowNum);

	row_header *api_row_header = new row_header();
	api_row_header->RID = api_row->RID();
	auto nullbits = api_row->nullbits();
	for(int j=0;j<(int)nullbits[0].size();j++) {
		api_row_header->nullbits.push_back(nullbits[0][j]);
	}
	
	// Assert that API row_header is the same as our row_header
	assert(api_row_header->RID == row->RID);
	for(int i=0; i<(int)row->nullbits.size(); i++)
		assert(api_row_header->nullbits[i] == row->nullbits[i]);
	
	delete api_row_header;
	printf("Row Header Assertion Passed!\n");
}

void lineitemAssertion(const Tables::Table *table, int rowNum, vector<uint8_t> &data) {
	lineitem *row1 = new lineitem();
	lineitem *row2 = new lineitem();

	// Get data row using flatbuffers api
	const flatbuffers::Vector<flatbuffers::Offset<Row>>* recs = table->rows();
	auto api_row = recs->Get(rowNum)->data_flexbuffer_root().AsVector();
	row1->orderkey = api_row[0].AsInt32();
	row1->partkey = api_row[1].AsInt32();
	row1->suppkey = api_row[2].AsInt32();
	row1->linenumber = api_row[3].AsInt32();
	row1->quantity = api_row[4].AsFloat();
	row1->extendedprice = api_row[5].AsFloat();
	row1->discount = api_row[6].AsFloat();
	row1->tax = api_row[7].AsFloat();
	row1->returnflag = api_row[8].AsInt8();
	row1->linestatus = api_row[9].AsInt8();
	row1->shipdate = api_row[10].AsString().str();
	row1->receiptdate = api_row[11].AsString().str();
	row1->commitdate = api_row[12].AsString().str();
	row1->shipinstruct = api_row[13].AsString().str();
	row1->shipmode = api_row[14].AsString().str();
	row1->comment = api_row[15].AsString().str();

	// Get data row using our api
	auto row = flexbuffers::GetRoot(data).AsVector();
	row2->orderkey = row[0].AsInt32();
	row2->partkey = row[1].AsInt32();
	row2->suppkey = row[2].AsInt32();
	row2->linenumber = row[3].AsInt32();
	row2->quantity = row[4].AsFloat();
	row2->extendedprice = row[5].AsFloat();
	row2->discount = row[6].AsFloat();
	row2->tax = row[7].AsFloat();
	row2->returnflag = row[8].AsInt8();
	row2->linestatus = row[9].AsInt8();
	row2->shipdate = row[10].AsString().str();
	row2->receiptdate = row[11].AsString().str();
	row2->commitdate = row[12].AsString().str();
	row2->shipinstruct = row[13].AsString().str();
	row2->shipmode = row[14].AsString().str();
	row2->comment = row[15].AsString().str();
	
	assert(row1->orderkey == row2->orderkey);
	assert(row1->partkey == row2->partkey);
	assert(row1->suppkey == row2->suppkey);
	assert(row1->linenumber == row2->linenumber);
	
	assert(row1->quantity == row2->quantity);
	assert(row1->extendedprice == row2->extendedprice);
	assert(row1->discount == row2->discount);
	assert(row1->tax == row2->tax);

	assert(row1->returnflag == row2->returnflag);
	assert(row1->linestatus == row2->linestatus);

	assert( strcmp( row1->shipdate.c_str(), row2->shipdate.c_str() ) == 0);
	assert( strcmp( row1->receiptdate.c_str(), row2->receiptdate.c_str() ) == 0);
	assert( strcmp( row1->commitdate.c_str(), row2->commitdate.c_str() ) == 0);

	assert( strcmp( row1->shipinstruct.c_str(), row2->shipinstruct.c_str() ) == 0);
	assert( strcmp( row1->shipmode.c_str(), row2->shipmode.c_str() ) == 0);
	assert( strcmp( row1->comment.c_str(), row2->comment.c_str() ) == 0);

	delete row1;
	delete row2;
	
	printf("Lineitem Data Assertion Passed!\n");
}
