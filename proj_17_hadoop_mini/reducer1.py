#! /usr/bin/python

import sys                                                                                                                                                                                                         
current_vin= None                                                                                                                                                                                                  
current_year=None                                                                                                                                                                                                  
current_make=None                                                                                                                                                                                                  
current_grpcnt =0                                                                                                                                                                                                  

def reset():                                                                                                                                                                                                       

    global current_grpcnt                                                                                                                                                                                          

    current_year = None                                                                                                                                                                                            
    current_make = None                                                                                                                                                                                            
    current_grpcnt = 0                                                                                                                                                                                             


# Run for end of every group                                                                                                                                                                                       
def flush():                                                                                                                                                                                                       
    for i in range(current_grpcnt):                                                                                                                                                                                
        print('%s,%s,%s'%( current_vin, current_make,  current_year) )                                                                                                                                             

# input comes from STDIN                                                                                                                                                                                           
for line_raw in sys.stdin:                                                                                                                                                                                         
    # [parse the input we got from mapper and update the master info]                                                                                                                                              
    line =line_raw.strip().split(',')                                                                                                                                                                              
    vin, inc_type, make, year = line[0],line[1],line[2],line[3]                                                                                                                                                    

    # [detect key changes]                                                                                                                                                                                         
    if current_vin != vin:                                                                                                                                                                                         
        if current_vin != None:                                                                                                                                                                                    
            # write result to STDOUT                                                                                                                                                                               
            if current_make==None or  current_year==None:                                                                                                                                                          
                print('data missing for {}'.format(current_vin))                                                                                                                                                   
            else:                                                                                                                                                                                                  
                flush()                                                                                                                                                                                            
        reset()                                                                                                                                                                                                    
    # [update more master info after the key change handling]                                                                                                                                                      

    if inc_type == 'A':                                                                                                                                                                                            
            current_grpcnt = current_grpcnt+1                                                                                                                                                                      

    if inc_type == 'I':                                                                                                                                                                                            
            current_make = make                                                                                                                                                                                    
            current_year = year                                                                                                                                                                                    
                                                                                                                                                                                                                   
    current_vin = vin                                                                                                                                                                                              
#  output the last group if needed!                                                                                                                                                                
flush()                                                                                                                                                                                                            
                
