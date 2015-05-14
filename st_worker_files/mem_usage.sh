#!/usr/bin/env bash 

## Print header
echo -e "Time\t\tSize\tResid.\tShared\tData\t%"
while [ 1 ]; do
    NOW=$(date +"%H:%M:%S")
    ## Get the PID of the process name given as argument 1
    pidno=`pgrep $1`
    ## If the process is running, print the memory usage
    if [ -e /proc/$pidno/statm ]; then
        ## Get the memory info
        m=`awk '{OFS="\t";print $1,$2,$3,$6}' /proc/$pidno/statm`
        ## Get the memory percentage
        perc=`top -bd .10 -p $pidno -n 1  | grep $pidno | gawk '{print \$10}'`
        ## print the results
        echo -e "$NOW\t$m\t$perc";
        ## If the process is not running
    else
        echo "$1 is not running";
    fi
    sleep 5
done
