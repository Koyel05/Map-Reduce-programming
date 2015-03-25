import MapReduce
import sys

"""
Mutual friends Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: a person
    # value: friends of the person
	key = record[0]
	value = record[1]
	for v in value:
		if key<v:
			mr.emit_intermediate((key+v),value)
		else:
			mr.emit_intermediate((v+key),value)


def reducer(key, list_of_values):
    # key: unique for each pair
    # value: list of friends for each person in pair
	if len(list_of_values)==2:
		v1 = list_of_values[0]
		v2 = list_of_values[1]
		l=[val for val in v1 if val in v2]
		if len(l)>0 :
			mr.emit((key,l))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
