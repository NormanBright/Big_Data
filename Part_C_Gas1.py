"""PartC_gas assignment
"""
# the goal of this program is to have an output of transaction in respect of time, this will be later compared with other grpahs in the report.
from mrjob.job import MRJob
import re
import operator
import time
#This line declares the class, that extends the MRJob format.
class PartC_gas1(MRJob):

# this class will define two additional methods: the mapper method goes here
    def mapper(self, _, line):
        try:
            if (len(line.split(","))==7):
                fields = line.split(",")
                address = fields[2]
                month_year = time.strftime("%Y-%m", time.gmtime(int(fields[6])))
                yield (month_year, (address,1
        except:
            pass

#and the reducer method goes after this line
    def reducer(self, key, values):
         i =[0xaa1a6e3e6ef20068f7f8d8c835d2d22fd5116444, 0xfa52274dd61e1643d2205169732f29114bc240b3, 0x7727e5113d1d161373623e5f49fd568b4f543a9e,0x209c4784ab1e8183cf58ca33cb740efbf3fc18ef,0x6fc82a5fe25a5cdb58bc74600a40a69c065263f8,0xbfc39b6f805a9e40e77291aff27aee3c96915bdd,0xe94b04a0fed112f3664e45adb2b8915693dd5ff3,0xbb9bc244d798123fde783fcc1c72d3bb8c189413, 0xabbb6bebfa05aa13e908eaa492bd7a8343760477, 0x341e790174e3a4d35b65fdc067b6b5634a61caea]
         addtrans = ''
         for value in values:
             if (value[1] == 1):
                 addcont = value[0]
             elif (value[1] == 2):
                 addtrans = value[0]
         if addcont == addtrans:
                 yield(key,addtrans)
        #yield (none, 1)
#this part of the python script tells to actually run the defined MapReduce job.
if __name__ == '__main__':
    PartC_gas1.JOBCONF= { 'mapreduce.job.reduces': '16' }
    PartC_gas1.run()
