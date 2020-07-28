"""Part A assignment
"""
from mrjob.job import MRJob
import re
import time

#This line declares the class, that extends the MRJob format.
class PartA(MRJob):

# this class will define two additional methods: the mapper method goes here
    def mapper(self, _, line):
        fields = line.split(",")
        try:
            if (len(fields)==9):
                month_year = time.strftime("%Y-%m", time.gmtime(int(fields[7])))
                transaction = int(fields[8])
                yield (month_year, transaction)
        except:
            pass

#and the reducer method goes after this line
    def reducer(self, key, values):
         monthtrans = 0
         for val in values:
             monthtrans += val
         yield (key, monthtrans)
        #yield (none, 1)
#this part of the python script tells to actually run the defined MapReduce job.
if __name__ == '__main__':
    PartA.run()
