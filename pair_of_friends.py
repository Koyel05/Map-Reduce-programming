import MapReduce
import sys

"""
Pair of friends in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
	# key: A person
	# value: friends of the person
	key = record[0]
	value = record[1]
	for v in value:
		if key<v:
			mr.emit_intermediate((key,v),1)
		else:
			mr.emit_intermediate((v,key),1)

def reducer(key, list_of_values):
	# key: unique key generated for each pair
	# value: list of occurrence counts
	count=0
	for v in list_of_values:
		count+=1
		if count==2:
			mr.emit((key))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
