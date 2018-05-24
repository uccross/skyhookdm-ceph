#!/bin/bash
outdir=$1
for nosds in '1'  '2' '4' '8' '16'
do
    echo ${nosds}
    
    for run in 'osds.log'
    do
        runlog=${nosds}${run}
        echo $runlog
        
        for dstat in 'osds.dstat-client0.log' 'osds.dstat-osd0.log' 'osds.dstat-osd1.log'
        do
            dstatlog=${nosds}${dstat}
            echo $dstatlog

            
            if [ -e $dstatlog ]
            then echo "exists";
            else echo "no exists"; continue
            fi
        
            echo "passed continue"
            
            cmdbase="python dstat-extract-query-time-range.py --dstatlog  ${dstatlog} --runlog ${runlog} --outdir ${outdir}"
            #echo $cmdbase
            
            for qname in 'a' 'b' 'c' 'd' 'e' 'f'
            do 
                for cls in 'use-cls' 'no-cls'
                do 
                    for cache in 'hot' 'cold'
                    do
                        for sel in '1' '10' '100' 'unique'
                        do
                    
                    
                    
                            cmd="${cmdbase} --query ${qname} --cls ${cls} --cache ${cache} --selectivity-pct ${sel}"
                            echo $cmd
                            echo ""
                            eval $cmd

                    
                        done #sel
                    done #cache
                done # cls
            done # qname
        done # dstatlog
    done #runlog
done # nosds
    