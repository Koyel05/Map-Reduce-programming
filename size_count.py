import MapReduce
import sys

"""
Word's size count Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
	# key: document identifier
	# value: document contents
	key = record[0]
	value = record[1]
	words = value.split()
	l = 0
	s = 0
	t = 0
	m = 0
	for w in words:
		if len(w)==1:
			t += 1
		elif len(w)>=2 and len(w)<=4:
			s += 1
		elif len(w)>=5 and len(w)<=9:
			m += 1
		else:
			l += 1
	mr.emit_intermediate(key,('tiny', t))
	mr.emit_intermediate(key,('small', s))
	mr.emit_intermediate(key,('medium', m))
	mr.emit_intermediate(key,('large', l))

def reducer(key, list_of_values):
	# key: document identifier
	# value: size type and counts for that type
	l = 0
	s = 0
	t = 0
	m = 0
	for v in list_of_values:
		if v[0] == 'large':
			l += v[1]
		elif v[0] == 'medium':
			m += v[1]
		elif v[0] == 'small':
			s += v[1]
		else:
			t += v[1]
	mr.emit((key, [('large',l),('medium',m),('small',s), ('tiny',t)]))

# Do not modify below this line
# =============================
if __name__ == '__main__':
	inputdata = open(sys.argv[1])
	mr.execute(inputdata, mapper, reducer)
