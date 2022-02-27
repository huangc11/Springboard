from pyspark import SparkContext

def extract_vin_key_value(x):
    """
    Function:  to extract vin, make and year from orginal data
    output:  key -- vin_number
             value --  make and year, along with the incident type.

    """
    attrs= x.split(",")
    #y = sc.parallelize(lst)
    return (attrs[2],{'make':attrs[3], 'year':attrs[5], 'inc_type':attrs[1]})


def populate_make(recs):
    '''
    Function: Within a group of vin_number, iterate through all the records and find the one that has
              the make and year available and capture it in group level master info.
              As we filter and output accident records, those records need to be modified adding the
               master info that we captured in the first iteration.
    '''
    make = ''
    year = ''

    # capture make and year as the group level master information
    for rec in (recs):
        if rec['inc_type']=='I':
            make = rec['make']
            year = rec['year']

    recs_acc =[]
    for rec in (recs):
        if rec['inc_type'] == 'A':
            rec['make'] = make
            rec['year'] = year
            recs_acc.append(rec)

    return (recs_acc)

def extract_make_key_value(x):
    """
    Function: count the number of records for each make and year combination,
    Output: key should be the combination of vehicle make and year.
            value should be the count of 1.
    """
    return (x['make'] + '-' + x['year'], 1)

def print_coll(coll, title='title'):
    """print out collecting results to screen"""
    print('----------'+title+'-----------------')
    for ele in coll:
        print(ele)


if __name__ == '__main__':

    sc = SparkContext("local", "My Application")
    raw_rdd = sc.textFile("data_postsale.csv")


    vin_kv = raw_rdd.map(lambda x: extract_vin_key_value(x))
    print_coll(vin_kv.collect(),'Vin extracted')

    enhance_make = vin_kv.groupByKey().flatMap(lambda kv: populate_make(kv[1]))
    print_coll(enhance_make.collect(),'Accident records filtered out with make,year popluated')

    yearmake_count = enhance_make.map(lambda x: extract_make_key_value(x))
    print_coll(yearmake_count.collect(),'New (key, value) got mapped' )


    final_count = yearmake_count.reduceByKey(lambda x, y: x + y)
    print_coll(final_count.collect(), 'Final counts')

    final_count.saveAsTextFile('output/spark_output')

