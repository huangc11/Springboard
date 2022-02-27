#! /usr/bin/python
"""mapper1.py"""                                                                                                                                                                                                   
import  sys                                                                                                                                                                                                        
for line_raw in sys.stdin:                                                                                                                                                                                         
    if line_raw.strip()=='q':                                                                                                                                                                                      
        break;                                                                                                                                                                                                     
    line =line_raw.strip().split(',')                                                                                                                                                                              
                                                                                                                                                                                                                   
    #print out: vin, inc_type, make, year                                                                                                                                                                          
    print('%s,%s,%s,%s'%(line[2], line[1], line[3], line[5]))                                                                                                                                                      
                                                                 
