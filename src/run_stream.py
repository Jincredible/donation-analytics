import os
import sys
import pandas as pd
from Records import Records



def donation_analytics(fn_contributions,fn_percentile,fn_output):

    #check if input files exist
    if os.path.exists(fn_contributions) and os.path.exists(fn_percentile):
        print os.path.basename(fn_contributions)
        print os.path.basename(fn_percentile)
    else:
        print 'input filenames do not exist'
        return

    record = Records()

    #read percentile file
    percentile = float(open(fn_percentile,'r').read())

    record.set_percentile(percentile)
    record.set_fn_output(fn_output)

    col_index_recipient = 0
    col_index_donor = 7
    col_index_zipcode = 10
    col_index_date = 13
    col_index_amount = 14
    col_index_other = 15

    col_index = [col_index_recipient, col_index_donor, col_index_zipcode, col_index_date, col_index_amount, col_index_other]

    col_name_recipient = 'CMTE_ID'
    col_name_donor = 'NAME'
    col_name_zipcode = 'ZIP_CODE'
    col_name_date = 'TRANSACTION_DT'
    col_name_amount = 'TRANSACTION_AMT'
    col_name_other = 'OTHER_ID'

    col_name = [col_name_recipient, col_name_donor, col_name_zipcode, col_name_date, col_name_amount, col_name_other]



    process(fn_contributions,col_index, col_name,record)


    return

def process(in_filename, in_cols, in_names,in_record):


    for line in pd.read_csv(in_filename,sep='|',header=None, chunksize=1,usecols=in_cols, names=in_names):
        #add row data to the Records instance of this object
        #print(line.loc[:,in_names[0]].item())
        #if isinstance(line,pd.DataFrame):
        #    print 'this line is a dataframe'
        #in_record.add_input_record(line.loc[:,in_names[0]].item(),
        #                            line.loc[:,in_names[1]].item(),
        #                            str(line.loc[:,in_names[2]].item()),
        #                            str(line.loc[:,in_names[3]].item()),
        #                            str(line.loc[:,in_names[4]].item()),
        #                            line.loc[:,in_names[5]].item())
        df_line = in_record.process_record(line.loc[:,in_names[0]].item(),
                                    line.loc[:,in_names[1]].item(),
                                    str(line.loc[:,in_names[2]].item()),
                                    str(line.loc[:,in_names[3]].item()),
                                    str(line.loc[:,in_names[4]].item()),
                                    line.loc[:,in_names[5]].item())
        print 'df_line:'
        print(df_line)
        print 'fn_output: ' + in_record.get_fn_output()
        if isinstance(df_line, pd.DataFrame):
            if not os.path.isfile(in_record.get_fn_output()):
                df_line.to_csv(in_record.get_fn_output(),sep='|',header =False, index=False)
            else: # else it exists so append without writing the header
                df_line.to_csv(in_record.get_fn_output(),sep='|',mode = 'a',header=False,index=False)


        in_record.print_df_input()

        in_record.print_df_output()
    #return

if __name__ == "__main__":
    fn_contributions = sys.argv[1]
    fn_percentile= sys.argv[2]
    fn_output= sys.argv[3]

    donation_analytics(fn_contributions,fn_percentile,fn_output)
