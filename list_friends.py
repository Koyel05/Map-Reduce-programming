import MapReduce
import sys

"""
List of friends Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
	# key: a person
	# value: his friend
	key = record[0]
	value = record[1]
	mr.emit_intermediate(key, value)

def reducer(key, list_of_values):
    # key: a person
    # value: list of his friends
	mr.emit((key, list_of_values))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
