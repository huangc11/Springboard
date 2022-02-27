#!/usr/bin/python                                                                                                                                                                                                  
"""mapper.py"""                                                                                                                                                                                                    
import  sys                                                                                                                                                                                                        
for line_raw in sys.stdin:                                                                                                                                                                                         
    line =line_raw.strip().split(',')                                                                                                                                                                              
    #print out: vin, inc_type, make, year                                                                                                                                                                          
    print('%s-%s,1'%(line[1],line[2]))      
