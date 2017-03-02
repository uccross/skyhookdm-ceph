#!/usr/bin/python

import sys,getopt,os,io
import struct,time,math
import calendar
from datetime import datetime, date

def usage(): 
    print (
        'Converts TPCH dbgen delimited data for any table into mutiple files' 
        ' of binary data for Ceph objects. Each file contains <rows> of data.'
        ' By default it assumes the schema of the'
        ' lineitem table. Schema is denoted below as char.1 for char(1).'
        ' ints are represented as 4 bytes.'
        ' date is converted to utc and written as int. '
        ' chars and varchars are truncated to their specified bytes. ' 
        ' decimal(15,2) is taken as a 15 character string and converted '
        ' to an 8 byte float, can be denoted as decimal.15 for decimal(15,2). \n '
        )
        
    print (
        'Usage: \n'
        'convert_tpch-tables-v1.py [args]\n' 
        ' non-parameterized args: \n' 
        ' -h[--help] prints this menu \n'   
        ' -d[--debug] (default=no)\n'
        ' -a[--align] use byte alignment (default=no)\n'   
        ' \n'
        ' parameterized args: \n'            
        ' -i[--input] <inputfile> (default=none)\n' 
        ' -r[--rows]  <rows_per_object> (default=1)\n'
        ' -m[--delim] <delimiter> (default=\'|\') \n'
        ' -s[--schema] <schema, ie. col types and widths> '
        '(default=int,int,int,int,decimal.15,decimal.15,decimal.15'
        'decimal.15,char.1,char.1,date,date,date,char.25,char.10,'
        'varchar.44)\n'
        )
        
    print(
        'Example:\n'
        'python convert-tpch-tables-v1.py -i lineitem-1K-rows.tbl -r 10 \n'
        'python convert-tpch-tables-v1.py -i some-table-csv.tbl -r 5000 -m , -s int,int,varchar.40,char.10,char.1,decimal.15,date -d\n'
        )
            
def main(argv):
    ifile = ''
    
    # defaults to the TPCH lineitem table 
    schema = ["int","int","int","int","decimal.15","decimal.15","decimal.15",
        "decimal.15","char.1","char.1","date","date","date","char.25",
        "char.10","varchar.44"]
    alignment = False # no byte aligned struct
    delim = '|'
    debug = False
    rows_per_object = 1
    
    try:
        opts, args = getopt.getopt(
            argv,
            "hbtadi:o:s:m:r:",
            ["input=","schema=","delim=","debug=","help=","rows=" ])
            
    except getopt.GetoptError:
        usage()
        sys.exit(2)
        
    for opt, arg in opts:
        if opt == '-h':
            usage()
            sys.exit()
        elif opt in ("-d", "--debug"):
            debug = True
        elif opt in ("-i", "--input"):
            ifile = arg
        elif opt in ("-s", "--schema"):
            schema = arg.split(',')     
        elif opt in ("-a", "--align"):
            alignment = True
        elif opt in ("-m", "--delim"):
            delim = arg
            assert len(delim) == 1, "delimiter must be a single character"
        elif opt in ("-r", "--rows"):
            rows_per_object = int(arg)
            
    assert len(ifile) > 0, "An input file name must be provided."
    print 'Input file is ', ifile
    print 'cols=>', ",".join(schema)
    
    # compute the input file size
    size = float(os.stat(ifile).st_size) / float(1024*1024)
    print ('input file size is {:d} {:}; {:.2f} {:}'
        .format(os.stat(ifile).st_size, 'Bytes', size, 'MB'))   

    # main loop:
    # for each line. we create an array of values, decoding them based on type
    # and adding to an array of vals that is used to create a struct for that 
    # line we write out the struct of vals, and its schema (packstring).

    outfile = None
    line_number = 0
    object_number = 0
    object_row_counter = 0
    with open(ifile) as infile:
        for line in infile:

            if debug:
                print "line_nummber=", line_number
                print "object_row_counter=", object_row_counter

            if (line_number == 0) or (line_number % rows_per_object) == 0:
                object_row_counter = 1
                if outfile is not None:
                    outfile.flush()
                    os.fsync(outfile.fileno())
                    outfile.close()
                    size = float(os.stat(outfile.name).st_size) / float(1024*1024)
                    print ('Done with {:}; file size is {:d} {:}; {:.2f} {:}'
                        .format(outfile.name, os.stat(outfile.name).st_size, 'Bytes', size, 'MB'))
                
                # create new object file.
                print "\nCreating new object number", object_number
                outfile = io.open(("obj" + str(object_number).zfill(7) + ".bin"), 'wb')
                object_number += 1
            line_number += 1
            object_row_counter += 1
                
            # split the line on the delim, assert row is complete according to 
            # schema provided.
            fields= line.split(delim)
            data = [f.strip() for f in fields]
            row = data[0:len(data)-1] # ignore the last split (it is empty)
            assert len(row) == len(schema), ( 
                ' line_number {:d} has {:d} cols, expected {:d} cols,'
                ' using delim=\'{:}\' line={:}\nbut parsed cols={:}'
                .format(line_number, len(row), len(schema), delim, line, row)
                )

            # start the packstring, which is appended to for each struct val 
            # and used for en/decode
            packstring = ""
            if alignment:
                packstring += "@"  # native alignment
            else:
                packstring += "="  # no alignment
            vals_list = []
            
            # convert each data type according to schema and update the packstring.
            for col in xrange(0,len(row)):   
                value = None
                format = None
                if debug:
                    print "schema {:}; val={:}".format(schema[col], row[col])
                
                # ints are converted to 4 byte ints and written as int 4bytes
                # expects 'int'
                if "int" in schema[col]:
                    value = int(row[col])
                    format = "i"
                
                # decimals are converted to 8 byte floats and written as float 8 bytes
                # expects: 'decimal.15' which will be taken as a
                # 15 char string then converted to float, ignoring precision.
                if "decimal" in schema[col]: 
                    n = int(schema[col].split('.')[1])
                    data = row[col][0:n]
                    value = float(data)
                    format = "q"
                    
                # chars are converted to fixed length strings and written as char n bytes
                # expects: 'char.2' or 'varchar.25' as format chars
                if "char" in schema[col]:
                    n = int(schema[col].split('.')[1])
                    data = row[col].ljust(n, ' ') # adds padding at end of string
                    value = data[0:n]
                    format = (str(n) + "s")
                    
                # dates are converted to utc and written as int 4 bytes
                # https://www.postgresql.org/docs/9.1/static/datatype-datetime.html
                # expects: 'date' as format 1996-02-12 
                if "date" in schema[col]:
                    dt = datetime.strptime(row[col], "%Y-%m-%d")
                    value = calendar.timegm(dt.utctimetuple())  # returns utc
                    format = "i"

                assert value is not None, "oops, fell through schema types."
                assert format is not None, "oops, fell through schema types."
                packstring += format
                vals_list.append(value)

            if debug:
                print ("writing " + outfile.name +"; object_row_counter=" 
                    + str(object_row_counter))
                
            # setup to write the data to binary file via struct bytes
            struct_list = []
            struct_list.append(packstring)
            struct_list.extend(vals_list)
            
            # see data types identifiers, https://docs.python.org/2/library/struct.html
            # example use: struct.pack(<packing_string>,<ordered data vals>)
            # data = struct.pack('=iii2siq', 1123456,2,3,"ss", 3, 4)
            # print 'data unpacked=', struct.unpack('=iii2siq', data)
            
            # write out the row as binary struct
            data = struct.pack(*struct_list)
            outfile.write(data)
            
            if debug:
                print 'data unpacked=', struct.unpack(packstring, data)
                
    print 'total lines of input processed=', line_number    
    
if __name__ == "__main__":
    main(sys.argv[1:])
        