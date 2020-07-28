"""Part_B
"""
from mrjob.job import MRJob
import re
import operator

#This line declares the class Lab1, that extends the MRJob format.
class Part_B(MRJob):

# this class will define two additional methods: the mapper method goes here
    def mapper(self, _, line):
        fields = line.split(",")
        try:
            if (len(fields)==7):
                #yield None, 1
                address = fields[2]
                addressvalue = int(fields[3])
                yield (address, addressvalue)
        except:
            pass

    def combiner(self, key, values):
        addcheck = sum(values)
        yield (key, addcheck)
#and the reducer method goes after this line
    def reducer(self, key, values):
        addcheck = sum(values)
        yield (key, addcheck)



# combiner same as reducer
#this part of the python script tells to actually run the defined MapReduce job. Note that Lab1 is the name of the class
if __name__ == '__main__':
    Part_B.JOBCONF= { 'mapreduce.job.reduces': '4' }
    Part_B.run()
