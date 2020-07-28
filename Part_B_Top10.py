"""Part_B
"""
from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import operator

#This line declares the class Lab1, that extends the MRJob format.
class Part_BTop10(MRJob):
    def steps(self):
        return [MRStep(mapper=self.mapper, reducer=self.reducer),]
        MRStep(reducer=self.reducer)
# this class will define two additional methods: the mapper method goes here
    def mapper(self, _, line):
        # fieldsTrans = row.split("\t")
        try:
            if (len(line.split("\t"))== 2):
                fields = line.split("\t")
                address = fields[0].replace('"','') #strip('"')
                addressvalue = int(fields[1])
                yield(_, (address,addressvalue))
        except:
            pass

#and the reducer method goes after this line
    def reducer(self, _,values):
        sortedValues = sorted(values, key=operator.itemgetter(1), reverse=True)
        rank = 1
        for item in sortedValues[:10]:
            yield(rank, '{}-{}'.format(*item))
            rank += 1




# combiner same as reducer

#this part of the python script tells to actually run the defined MapReduce job. Note that Lab1 is the name of the class
if __name__ == '__main__':
    Part_BTop10.JOBCONF= { 'mapreduce.job.reduces': '16' }
    Part_BTop10.run()
