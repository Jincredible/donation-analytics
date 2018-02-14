import pandas as pd
import numpy as np

class Records(object):
    #The records object holds 2 dataframes and 2 hash tables:
    #1. df_input: input dataframe, of raw data of relevant files from the input files
    #2a. set_input: set to store individual, recipient, zipcode and date data
    #2b. hash_unique: hastable to store all zipcode-year-recipient data
    #3. output dataframe



    def __init__(self):
        self._input_columns = ['CMTE_ID','NAME','ZIP_CODE','TRANSACTION_DT','TRANSACTION_AMT','OTHER_ID']
        self._output_columns = ['recipient', 'zipcode', 'year', 'quantile','sum','count']
        #create dataframes
        self._df_input = pd.DataFrame(columns=self._input_columns)
        #self._df_input = pd.DataFrame()
        self._set_input = set()
        self._hash_unique = dict()
        self._df_output = pd.DataFrame(columns=self._output_columns)

    def set_percentile(self,in_percent):
        self._percentile = in_percent

    def get_percentile(self):
        return self._percentile

    def set_fn_output(self,in_filename):
        self._fn_output = in_filename

    def get_output_columns(self):
        return self._output_columns

    def get_fn_output(self):
        return self._fn_output

    def print_df_input(self):
        print 'record df inputs:'
        print(self._df_input)
        return

    def print_df_output(self):
        print 'record df outputs:'
        print(self._df_output)
        return

    def process_record(self, in_recipient, in_name, in_zip, in_date, in_amount, in_other):
        if self.check_input(in_name+'-'+in_zip+'-'+in_recipient+'-'+in_date) or isinstance(in_other, str):
            return
        else:
            temp_len = len(self._df_input.index)
            self._df_input = self._df_input.append(pd.DataFrame(dict(zip(self._input_columns,[in_recipient, in_name, in_zip, in_date, in_amount, in_other])),index=[temp_len]))
            if self.check_unique(in_recipient+'-'+Records.fix_zipcode(in_zip), in_amount):
                #Is unique
                return
            else:
                #isn't unique, add it to the output dataframe
                out_quantile = self.calc_quantile(self._hash_unique.get(in_recipient+'-'+Records.fix_zipcode(in_zip)))
                out_sum = self.calc_sum(self._hash_unique.get(in_recipient+'-'+Records.fix_zipcode(in_zip))) #make a getsum function, ignore first value
                out_count = self.calc_count(self._hash_unique.get(in_recipient+'-'+Records.fix_zipcode(in_zip)))
                temp_len = len(self._df_output.index)

                temp_df = pd.DataFrame(dict(zip(self._output_columns,
                                [in_recipient,Records.fix_zipcode(in_zip),Records.year_from_date(in_date), out_quantile, out_sum, out_count])),index=[temp_len])

                temp_df = temp_df[self._output_columns]

                self._df_output=self._df_output.append(temp_df)
                return temp_df
        return

    def add_input_record(self, in_recipient, in_name, in_zip, in_date, in_amount, in_other):
        #print 'running add_input_record: ' + in_name+'-'+in_zip+'-'+in_recipient+'-'+in_date
        if self.check_input(in_name+'-'+in_zip+'-'+in_recipient+'-'+in_date) or isinstance(in_other, str):

            #the check_input function indicates that that combination of donor, recipient and date exists in set
            #or the value in the other column is not null
            #print '   combination exists in set or the other column is not null'
            return
        else:
            #print '   combination does not exists in set and the other column is null'
            #first, add the record to the input DataFrame

            temp_len = len(self._df_input.index)
            #print 'temp_len: ' + str(temp_len)
            self._df_input = self._df_input.append(pd.DataFrame(dict(zip(self._input_columns,[in_recipient, in_name, in_zip, in_date, in_amount, in_other])),index=[temp_len]))

            #then, add the amount to the hash table for checking uniques
            if self.check_unique(in_recipient+'-'+Records.fix_zipcode(in_zip), in_amount):
                #Is unique
                #print '      hash table check: is unique'
                return
            else:
                #print '      hash table check: is NOT unique, add to output dataframe'
                #isn't unique, add it to the output dataframe
                self.add_output_record(in_recipient, in_name, in_zip, in_date, in_amount, in_other)
                return

    def add_output_record(self, in_recipient, in_name, in_zip, in_date, in_amount, in_other):
        #need to fix!
        #out_quantile = np.percentile(np.array(self._has_unique.get(in_recipient+'-'+Records.fix_zipcode(in_zip)+'-'+Records.year_from_date(in_date)))),self.get_percentile())
        out_quantile = self.calc_quantile(self._hash_unique.get(in_recipient+'-'+Records.fix_zipcode(in_zip)))

        out_sum = self.calc_sum(self._hash_unique.get(in_recipient+'-'+Records.fix_zipcode(in_zip))) #make a getsum function, ignore first value
        out_count = self.calc_count(self._hash_unique.get(in_recipient+'-'+Records.fix_zipcode(in_zip))) #make a get count function, ignore first value
        #print 'adding to self._df_output... recipient: ' + in_recipient + '  zip:   ' + Records.fix_zipcode(in_zip) + '  year:  ' + Records.year_from_date(in_date)
        #print '     sum:  ' + str(out_sum) + '   count: ' + str(out_count)

        temp_len = len(self._df_output.index)

        self._df_output=self._df_output.append(pd.DataFrame(dict(zip(self._output_columns,
                        [in_recipient,Records.fix_zipcode(in_zip),Records.year_from_date(in_date), out_quantile, out_sum, out_count])),index=[temp_len]))
                #columns=self._output_columns

        return

    def calc_quantile(self, in_list):
        return np.percentile(np.array(in_list[1:]),self.get_percentile(),interpolation='nearest')

    def calc_sum(self, in_list):
        return sum(in_list[1:])

    def calc_count(self, in_list):
        return len(in_list[1:])

    def check_input(self,in_index):
        if in_index in self._set_input:
            return True
        else:
            self._set_input.add(in_index)
            return False

    def check_unique(self, in_key, in_amount):
        #print 'checking for key: ' + in_key
        #self.print_hashkeys()
        if in_key in self._hash_unique:
            #This means that the key is not unique, is a duplicate
            self._hash_unique[in_key].append(int(in_amount))
            return False
        else:
            self._hash_unique[in_key]=[int(in_amount)]
            return True

    def print_dict(self, in_dict):
        for key in in_dict:
            print 'key: ' + str(key) + ' value: ' + str(in_dict[key])
        return

    def print_hashkeys(self):
        #print 'printing keys for dict of len: ' + str(len(self._hash_unique))
        for key in self._hash_unique:
            print 'key: ' + key + ' value: ' + str(self._hash_unique[key])
        return

    @staticmethod
    def year_from_date(in_date):
        return in_date[-4:]
        #in_date.astype(str).str[-4:].astype(np.int64)

    @staticmethod
    def fix_zipcode(in_zip):
        return str(int(in_zip)%(10**5)).zfill(5)
        #int(in_zip).mod(10**5).dropna().astype(str).str.zfill(5)
