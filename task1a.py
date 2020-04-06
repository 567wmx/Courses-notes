import sys
import csv
import io
from pyspark import SparkContext


def csv_separate_format(x):
    output = io.StringIO("")
    csv.writer(output).writerow(x)
    return output.getvalue().strip()


if __name__ == '__main__':
    sc = SparkContext()
    trip_rdd = sc.textFile(sys.argv[1], 1)
    trip_rdd = trip_rdd.mapPartitions(lambda x: csv.reader(x))

    fare_rdd = sc.textFile(sys.argv[2], 1)
    fare_rdd = fare_rdd.mapPartitions(lambda x: csv.reader(x))

    fares_header = fare_rdd.first()
    fare_rdd = fare_rdd.filter(lambda line: line != fares_header)

    trips_header = trip_rdd.first()
    trip_rdd = trip_rdd.filter(lambda line: line != trips_header)

    fare_rdd = fare_rdd.map(lambda x: (x[0] + "#" + x[1] + "#" + x[2] + "#" + x[3], \
                                       x[4] + "#" + x[5] + "#" + x[6] + "#" + x[7] + "#" + x[8] + "#" +
                                       x[9] + "#" + x[10]))
    trip_rdd = trip_rdd.map(lambda x: (x[0] + "#" + x[1] + "#" + x[2] + "#" + x[5], \
                                       x[3] + "#" + x[4] + "#" + x[6] + "#" + x[7] + "#" + x[8] + "#" + \
                                       x[9] + "#" + x[10] + "#" + x[11] + "#" + x[12] + "#" + x[13]))
    result = trip_rdd.join(fare_rdd)
    result = result.map(lambda x: str(x[0]) + "#" + str(x[1][0]) + "#" + str(x[1][1]))
    result = result.map(lambda line : line.split('#'))
    result = result.sortBy(lambda x: ",".join([x[0],x[1],x[3]]))
    output = result.map(csv_separate_format)
    output.saveAsTextFile('task1a.out')
    sc.stop()

