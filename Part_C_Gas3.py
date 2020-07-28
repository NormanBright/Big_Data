"""PartC_gas3 assignment
"""
from mrjob.job import MRJob
import re
import operator
import time

#This line declares the class Lab1, that extends the MRJob format.
class PartC_gas3(MRJob):

# this class will define two additional methods: the mapper method goes here
    def mapper(self, _, line):
        fields = line.split(",")
        try:
            if (len(fields)==7):
                #yield None, 1
                month_year = time.strftime("%Y-%m", time.gmtime(int(fields[6])))
                gas_price = int(fields[5])
                yield (month_year, gas_price)
        except:
            pass

#and the reducer method goes after this line
    def reducer(self, key, values):
        gas_pricevalue = 0
        for val in values:
            gas_pricevalue +=val
        yield (key, gas_pricevalue)

#this part of the python script tells to actually run the defined MapReduce job. Note that Lab1 is the name of the class
if __name__ == '__main__':
    PartC_gas3.JOBCONF= { 'mapreduce.job.reduces': '16' }
    PartC_gas3.run()
