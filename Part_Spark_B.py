import re
import pyspark


sc = pyspark.SparkContext()


# we will use this function later in our filter transformation
def transactions(line):
    try:
        fields = line.split(',')
        if len(fields) != 7:
            return False
        int(fields[3])
        return True
    except:
        pass


def contracts(line):
    try:
        fields = line.split(',')
        if len(fields) != 5:
            return False
        return True
    except:
        pass





transaction = sc.textFile("/data/ethereum/transactions")
contracts = sc.textFile("/data/ethereum/contracts")

transaction_filter = transaction.filter(transactions)
transaction_join = transaction_filter.map(lambda field_num: (field_num.split(",")[2], int(field_num.split(",")[3])))
features = transaction_join.reduceByKey(lambda a, b: a + b) #aggregation

contracts_filter = contracts.filter(contracts) #apply the function applied above and clean the data
contracts_join = contracts_filter.map(lambda field_num: (field_num.split(",")[0], field_num.split(",")[0])) #take the field as key value pairs

join = features.join(contracts_join) #this actually join


top10 = join.takeOrdered(10, key=lambda x: -x[1][0]) #top10

for record in top10:
    print("{} : {}".format(record[0], record[1][0])) #print the top10
