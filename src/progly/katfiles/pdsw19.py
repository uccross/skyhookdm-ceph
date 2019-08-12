import random
import string
import os
import numpy as np

def dsgen( arity, numrows ) :

  # arity3 ----------------------------- #
  if arity == 3 :
    outfile_name = 'dataset_arity3_' + str( numrows ) + '_rows.txt'
    os.system( 'rm ' + outfile_name )
    outfile = open( outfile_name, 'w' )

    print "writing " + str( numrows ) + " ints to file '" + outfile_name + "'"
    for i in range( 0, numrows ) :
      anint   = np.uint32( random.randint( 0, 100 ) )
      afloat  = np.float32( random.random() )
      basestr = ''.join( [ random.choice( string.ascii_letters + string.digits ) for n in xrange( 10 ) ] )
  
      extra_digits = random.randint( 0, 4 )
      astr1  = ''.join( [ random.choice( string.ascii_letters + string.digits ) for n in xrange( extra_digits ) ] )
      astr   = basestr+astr1
  
      arow = str( anint ) + "|" + str( afloat ) + "|" + astr
  
      print outfile_name + " : writing row : " + str( arow )
      outfile.write( arow + "\n" )

  # arity4 ----------------------------- #
  elif arity == 4 :
    outfile_name = 'dataset_arity4_' + str( numrows ) + '_rows.txt'
    os.system( 'rm ' + outfile_name )
    outfile = open( outfile_name, 'w' )

    print "writing " + str( numrows ) + " ints to file '" + outfile_name + "'"

    for i in range( 0, numrows ) :
      anint0  = np.uint32( random.randint( 0, 100 ) )
      anint1  = np.uint32( random.randint( 0, 100 ) )
      afloat  = np.float32( random.random() )
      basestr = ''.join( [ random.choice( string.ascii_letters + string.digits ) for n in xrange( 10 ) ] )
  
      extra_digits = random.randint( 0, 4 )
      astr1  = ''.join( [ random.choice( string.ascii_letters + string.digits ) for n in xrange( extra_digits ) ] )
      astr   = basestr+astr1
  
      arow = str( anint0 ) + "|" + str( anint1 ) + "|" + str( afloat ) + "|" + astr
  
      print outfile_name + " : writing row : " + str( arow )
      outfile.write( arow + "\n" )

  # wut? ----------------------------- #
  else :
    print "artiy '" + str( arity ) + "' not supported."

  outfile.close()

def main() :
  #numrows_arity3 = 50000   # 1mb
  #numrows_arity3 = 500000  # 10mb
  numrows_arity3 = 5000000 # 100mb
  dsgen( 3, numrows_arity3 ) # arity 3

  #numrows_arity4 = 42000   # 1mb
  #numrows_arity4 = 420000  # 10mb
  numrows_arity4 = 4200000 # 100mb
  dsgen( 4, numrows_arity4 ) # arity 4

if __name__ == "__main__" :
  main()
