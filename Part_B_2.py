"""Part_B
"""
from mrjob.job import MRJob
import re
import operator

#This line declares the class Lab1, that extends the MRJob format.
class Part_B2(MRJob):

# this class will define two additional methods: the mapper method goes here
    def mapper(self, _, line):
        # fieldsTrans = row.split("\t")
        try:
            if (len(line.split(","))==5):
                fields = line.split(",")
                address = fields[0]
                blockNumber = fields[3]
                yield(address, (blockNumber, 1)) #1 represents from Contracts
            elif (len(line.split("\t"))== 2):
                fields = line.split("\t")
                address = fields[0].replace('"','') #strip('"')
                addressvalue = int(fields[1])
                yield(address,(addressvalue, 2)) #2 represents transactions aggregate
        except:
            pass

#and the reducer method goes after this line
    def reducer(self, key, values):
        block = ''
        trans = 0
        for value in values:
            if (value[1] == 1):
                block = value[0]
            elif (value[1] == 2):
                trans = value[0]
        if block != '' and trans > 0:
            yield(key,trans)



# combiner same as reducer

#this part of the python script tells to actually run the defined MapReduce job. Note that Lab1 is the name of the class
if __name__ == '__main__':
    Part_B2.JOBCONF= { 'mapreduce.job.reduces': '16' }
    Part_B2.run()
