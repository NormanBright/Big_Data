"""PartC_gas2 assignment
"""
from mrjob.job import MRJob
import re
import operator
import time

#This line declares the class Lab1, that extends the MRJob format.
class PartC_gas2(MRJob):

# this class will define two additional methods: the mapper method goes here
    def mapper(self, _, line):
        fields = line.split(",")
        try:
            if (len(fields)==7):
                #yield None, 1
                month_year = time.strftime("%Y-%m", time.gmtime(int(fields[6])))
                gas = int(fields[4])
                yield (month_year, gas)
        except:
            pass

#and the reducer method goes after this line
    def reducer(self, key, values):
        gasvalue = 0
        for val in values:
            gasvalue +=val
        yield (key, gasvalue)

#this part of the python script tells to actually run the defined MapReduce job. Note that Lab1 is the name of the class
if __name__ == '__main__':
    PartC_gas2.JOBCONF= { 'mapreduce.job.reduces': '16' }
    PartC_gas2.run()
