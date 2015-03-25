import MapReduce
import sys

"""
Relational join in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
	# key: relation name
	# value: relation's contents
	key = record[0]
	value = record[1:]
	if key=="MovieNames":
		mr.emit_intermediate(value[1], record)
	elif key=="MovieRatings":
		mr.emit_intermediate(value[0], record)

def reducer(key, list_of_values):
	# key: unique key of relation
	# value: all values in relation including its name
	v1=list_of_values[0]
	sum=0
	num=0
	a=v1[1:]
	for v2 in list_of_values[1:]:
		b=v2[1:]
		sum=sum+v2[3]
		mr.emit( (a+b) )
	avg=sum/(len(list_of_values[1:]))
	mr.emit((v1[1],avg))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
