import sys
import redis
import csv

def main(args):
  r = redis.Redis('localhost')
  pipe = r.pipeline()

  fileName = sys.argv[1]
  headerRead = False
  timeMap = {}
  with open(fileName, 'r') as file:
    reader = csv.reader(file, delimiter=',', quotechar='|')
    for row in reader:
      if headerRead:
        medallion = row[0]
        dropOffDatetime = row[6]
        tripTime = row[8]
        pipe.set('m:' + medallion + ':' + dropOffDatetime, tripTime)
      headerRead = True
  pipe.execute()
  print(timeMap)

if __name__ == '__main__':
  main(sys.argv[1:])