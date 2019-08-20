import random
import string
import os
import numpy as np

def dsgen( outfile_name, arity, numrows ) :

  # python range has upper limit.
  # need to divide the number of lines in the file
  # into something smaller and then loop around 
  # the number of divisions.
  splitter = 100000

  os.system( 'rm ' + outfile_name )
  outfile = open( outfile_name, 'a' )

  # arity3 (int,float,str) ----------------------------- #
  if arity == 3 :
    print "writing " + str( numrows ) + " ints to file '" + outfile_name + "'"
    counter = 0
    for k in range( 0, numrows / splitter ) :
      for i in range( 0, splitter ) :
        anint   = np.uint32( random.randint( 0, 100 ) )
        afloat  = np.float32( random.random() )
        basestr = ''.join( [ random.choice( string.ascii_letters + string.digits ) for n in xrange( 10 ) ] )
  
        extra_digits = random.randint( 0, 4 )
        astr1  = ''.join( [ random.choice( string.ascii_letters + string.digits ) for n in xrange( extra_digits ) ] )
        astr   = basestr+astr1
  
        arow = str( anint ) + "|" + str( afloat ) + "|" + astr
  
        counter += 1
        outfile.write( arow + "\n" )
        if( counter % 1000 == 0 ) :
          print outfile_name + " : writing row #" + str( counter )

  # arity4 (int,int,float,str) ----------------------------- #
  elif arity == 4 :
    print "writing " + str( numrows ) + " ints to file '" + outfile_name + "'"
    counter = 0
    for k in range( 0, numrows / splitter ) :
      for i in range( 0, splitter ) :
        anint0  = np.uint32( random.randint( 0, 100 ) )
        anint1  = np.uint32( random.randint( 0, 100 ) )
        afloat  = np.float32( random.random() )
        basestr = ''.join( [ random.choice( string.ascii_letters + string.digits ) for n in xrange( 10 ) ] )
  
        extra_digits = random.randint( 0, 4 )
        astr1  = ''.join( [ random.choice( string.ascii_letters + string.digits ) for n in xrange( extra_digits ) ] )
        astr   = basestr+astr1
  
        arow = str( anint0 ) + "|" + str( anint1 ) + "|" + str( afloat ) + "|" + astr
  
        counter += 1
        outfile.write( arow + "\n" )
        if( counter % 1000 == 0 ) :
          print outfile_name + " : writing row #" + str( counter )

  # arity100 (int,int,...,int) ----------------------------- #
  elif arity == 100 :
    print "writing " + str( numrows ) + " ints to file '" + outfile_name + "'"
    counter = 0
    for k in range( 0, numrows / splitter ) :
      for i in range( 0, splitter ) :
        arow = ""
        first = True
        for j in range( 0, arity ) :
          if( first == True ) :
            first = False
          else :
            arow = arow + "|"
          arow = arow + str( np.uint32( random.randint( 10000000, 99999999 ) ) )

        counter += 1
        outfile.write( arow + "\n" )
        if( counter % 1000 == 0 ) :
          print outfile_name + " : writing row #" + str( counter )

  # wut? ----------------------------- #
  else :
    print "artiy '" + str( arity ) + "' not supported."

  outfile.close()

def main() :
  #numrows_arity3 = 50000   # 1mb
  #numrows_arity3 = 500000  # 10mb
  #numrows_arity3 = 5000000 # 100mb
  numrows_arity3 = 5000000000 # 100gb
  outfile_name = 'dataset_arity3_' + str( numrows_arity3 ) + '_rows.txt'
  dsgen( outfile_name, 3, numrows_arity3 ) # arity 3

  #numrows_arity4 = 42000   # 1mb
  #numrows_arity4 = 420000  # 10mb
  #numrows_arity4 = 4200000 # 100mb
  numrows_arity4 = 4200000000 # 100gb
  outfile_name = 'dataset_arity4_' + str( numrows_arity4 ) + '_rows.txt'
  dsgen( outfile_name, 4, numrows_arity4 ) # arity 4

  numrows_arity100 = 250000000 # 100gb
  outfile_name = 'dataset_arity100_' + str( numrows_arity100 ) + '_rows.txt'
  dsgen( outfile_name, 100, numrows_arity100 ) # arity100 

if __name__ == "__main__" :
  main()
