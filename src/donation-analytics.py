#takes in three inputs
#   1. contributions text file
#   2. percentile text file
#   3. output text file

import os
import sys
import numpy as np
import pandas as pd

col_index_recipient = 0
col_index_donor = 7
col_index_zipcode = 10
col_index_date = 13
col_index_amount = 14
col_index_other = 15

col_name_recipient = 'CMTE_ID'
col_name_donor = 'NAME'
col_name_zipcode = 'ZIP_CODE'
col_name_date = 'TRANSACTION_DT'
col_name_amount = 'TRANSACTION_AMT'
col_name_other = 'OTHER_ID'
col_name_year = 'YEAR'

def donation_analytics(fn_contributions,fn_percentile,fn_output):

    #check if input files exist
    if os.path.exists(fn_contributions) and os.path.exists(fn_percentile):

        print os.path.basename(fn_contributions)
        print os.path.basename(fn_percentile)
    #else #if not, catch error

    #read percentile file
    percentile = float(open(fn_percentile,'r').read())
    #print 'percentile variable is: ', percentile


    #read fn_contributions into a dataframe
    df_contributions = read_contributions(fn_contributions)
    #print 'printing output from read_contributions function:'
    #print(df_contributions)

    #print 'printing output from filter_contributions function:'
    #df_contributions = filter_contributions(df_contributions)
    #print (df_contributions)

    #print 'printing output from calc_contributions function:'
    #print calc_contributions(df_contribution)
    df_out = calc_contributions(df_contributions[filter_other(df_contributions[col_name_other]) & filter_duplicate(df_contributions)])
    #df_out = calc_contributions(filter_contributions(df_contributions))
    print 'printing output from calc_contributions function:'
    print (df_out)

    #the open function creates the file if it doesn't work
    #file_output = open(fn_output, 'w+')
    return


def read_contributions(fn):

    col_index = [col_index_recipient,col_index_donor,col_index_zipcode,
                    col_index_date,col_index_amount,col_index_other]

    names_index = [col_name_recipient,1,2,3,4,5,6,col_name_donor,8,9,
                    col_name_zipcode,11,12,col_name_date,col_name_amount,
                    col_name_other,16,17,18,19,20]

    df_out = pd.read_csv(fn,sep='|',header=None, names=names_index,
                            index_col=False).iloc[:,col_index]

    df_out[col_name_zipcode] = fix_zipcode(df_out[col_name_zipcode])
    df_out[col_name_year] = fix_year(df_out[col_name_date])


    return df_out

def fix_zipcode(s_in):
    #input is a single column of zipcodes
    #this function takes only the first five digits of the zipcodes values
    #.astype(np.int64)
    return s_in.mod(10**5).dropna().astype(str).str.zfill(5)

def fix_year(s_in):
    #input is the date column from the dataframe
    #returns a column only of the year
    return s_in.astype(str).str[-4:].astype(np.int64)

def filter_other(s_in):
    #input is the other column from the dataframe
    #returns a column of booleans where TRUE when the value is NaN

    return pd.isnull(s_in)

def filter_duplicate(df_in):
    #input is the dataframe
    #returns a column of booleans where TRUE when the value is duplicated
    return df_in.duplicated(subset=[col_name_donor,col_name_zipcode], keep='first')

def filter_contributions(df_in):
    #input is the dataframe output by the read_contributions function

    #first, test membership for values in the 'OTHER_ID' column is NaN
    filter_other = pd.isnull(df_in[col_name_other])
    #print 'Debug in filter_contributions function: printing filter_other...'
    #print(filter_other)

    filter_duplicate = df_in.duplicated(subset=[col_name_donor,col_name_zipcode],
                                        keep='first')
    #print 'Debug in filter_contributions function: printing filter_duplicate...'
    #print(filter_duplicate)

    return df_in[filter_other & filter_duplicate]

def calc_contributions(df_in):

    df_grouped = df_in.groupby([col_name_recipient,col_name_year,col_name_zipcode])
#    print 'printing df_grouped from calc_contributions function:'
#    for key, item in df_grouped:
#        print df_grouped.get_group(key), "\n\n"

    col_count = df_grouped.cumcount() + 1
    col_count.name = 'COUNT'

    col_sum = df_grouped[col_name_amount].cumsum()
    col_sum.name = 'SUM'
    df_out = pd.concat([df_in.loc[:,[col_name_recipient,col_name_zipcode,col_name_year]],col_sum,col_count],axis=1)

    #print 'Debug in calc_contributions function: printing col_count...'
    #print(col_count)

    #print 'Debug in calc_contributions function: printing col_sum...'
    #print(col_sum)

    return df_out

def calc_quantile(df_in,quantile_in):

    df_grouped = df_in.groupby([col_name_recipient,col_name_year,col_name_zipcode])

    return

if __name__ == "__main__":
    fn_contributions = sys.argv[1]
    fn_percentile= sys.argv[2]
    fn_output= sys.argv[3]

    donation_analytics(fn_contributions,fn_percentile,fn_output)
