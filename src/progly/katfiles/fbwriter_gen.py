import os
import sys

# get filenames in directory

path = sys.argv[1]

files = []
# r=root, d=directories, f = files
for r, d, f in os.walk(path):
  for filename in f:
    #if filename.startswith( "dataset_arity3_50000_rows-" ) :
    #if filename.startswith( "dataset_arity4_42000_rows-" ) :
    #if filename.startswith( "dataset_arity3_500000_rows-" ) :
    #if filename.startswith( "dataset_arity3_5000000_rows-" ) :
    #if filename.startswith( "dataset_arity100_2500_rows-" ) :
    #if filename.startswith( "dataset_arity100_25000_rows-" ) :
    if filename.startswith( "dataset_arity100_250000_rows-" ) :
      files.append(os.path.join(r, filename))

# loop over file names and execute fbwriter_fbu
obj_counter = 0
for f in files:
  print(f)
  testid = "fbxrows-arity100-10mb-100objs"
  os.system( \
     'bin/fbwriter \
     --file_name ' + f + ' \
     --schema_file_name /mnt/storage3/kat/arity100_schema.txt \
     --num_objs 1 \
     --flush_rows 250000 \
     --read_rows 249999 \
     --csv_delim "|" \
     --use_hashing false \
     --rid_start_value 1 \
     --table_name ' + testid + ' \
     --input_oid ' + str( obj_counter ) + ' \
     --obj_type SFT_FLATBUF_FLEX_ROW ;' )
#  if obj_counter == 2 :
#    break
  obj_counter += 1

#testid = "fbxrows-arity3-100mb-100objs"
#  os.system( \
#     'bin/fbwriter \
#     --file_name ' + f + ' \
#     --schema_file_name /mnt/storage3/kat/arity3_schema.txt \
#     --num_objs 1 \
#     --flush_rows 5000000 \
#     --read_rows 4999999 \
#     --csv_delim "|" \
#     --use_hashing false \
#     --rid_start_value 1 \
#     --table_name ' + testid + ' \
#     --input_oid ' + str( obj_counter ) + ' \
#     --obj_type SFT_FLATBUF_FLEX_ROW ;' )

#testid = "fbxrows-arity100-10mb-100objs"
#  os.system( \
#     'bin/fbwriter \
#     --file_name ' + f + ' \
#     --schema_file_name /mnt/storage3/kat/arity100_schema.txt \
#     --num_objs 1 \
#     --flush_rows 25000 \
#     --read_rows 24999 \
#     --csv_delim "|" \
#     --use_hashing false \
#     --rid_start_value 1 \
#     --table_name ' + testid + ' \
#     --input_oid ' + str( obj_counter ) + ' \
#     --obj_type SFT_FLATBUF_FLEX_ROW ;' )

#testid = "fbxrows-arity100-1mb-100objs"
#  os.system( \
#     'bin/fbwriter \
#     --file_name ' + f + ' \
#     --schema_file_name /mnt/storage3/kat/arity100_schema.txt \
#     --num_objs 1 \
#     --flush_rows 2500 \
#     --read_rows 2499 \
#     --csv_delim "|" \
#     --use_hashing false \
#     --rid_start_value 1 \
#     --table_name ' + testid + ' \
#     --input_oid ' + str( obj_counter ) + ' \
#     --obj_type SFT_FLATBUF_FLEX_ROW ;' )

#  fbxrows-arity3-10mb-100objs
#  os.system( \
#     'bin/fbwriter \
#     --file_name ' + f + ' \
#     --schema_file_name /mnt/storage3/kat/arity3_schema.txt \
#     --num_objs 1 \
#     --flush_rows 500000 \
#     --read_rows 499999 \
#     --csv_delim "|" \
#     --use_hashing false \
#     --rid_start_value 1 \
#     --table_name ' + testid + ' \
#     --input_oid ' + str( obj_counter ) + ' \
#     --obj_type SFT_FLATBUF_FLEX_ROW ;' )

#  fbxrows-arity4-1mb-100objs
#  os.system( \
#    'bin/fbwriter \
#     --file_name ' + f + ' \
#     --schema_file_name /mnt/storage3/kat/arity4_schema.txt \
#     --num_objs 1 \
#     --flush_rows 42000 \
#     --read_rows 41999 \
#     --csv_delim "|" \
#     --use_hashing false \
#     --rid_start_value 1 \
#     --table_name ' + testid + ' \
#     --input_oid ' + str( obj_counter ) + ' \
#     --obj_type SFT_FLATBUF_FLEX_ROW ;' )

#  fbxrows-arity3-1mb-100objs
#  os.system( \
#    'bin/fbwriter \
#     --file_name ' + f + ' \
#     --schema_file_name /mnt/storage3/kat/arity3_schema.txt \
#     --num_objs 1 \
#     --flush_rows 50000 \
#     --read_rows 49999 \
#     --csv_delim "|" \
#     --use_hashing false \
#     --rid_start_value 1 \
#     --table_name testdata \
#     --input_oid ' + str( obj_counter ) + ' \
#     --obj_type SFT_FLATBUF_FLEX_ROW ;' )
