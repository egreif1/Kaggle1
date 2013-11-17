# This script runs the sql analysis for the weather kaggle

# dependence
import imp
import string
import sys
import pymysql


# global variables
text = None
functional = None
sql = None
cur = None
con = None



# function to run on each row of
# the tweets table
def sum_data(line):

	# unpack data
	tweet = line[1].split(" ")
	s = line[2]
	w = line[3]
	k = line[4]
	state = str(line[5])


	# check that data is clean
	if not(state=="-1" or bad_id(state)):
		add_state_data(state,s,w,k)

		for word in tweet:
			if (not(text.is_emoticon(word))):
				word = text.strip_punc_all(word)
			if (not(bad_id(word))):
				add_word_data(word,s,w,k)


# check for bad characters in string
def bad_id(string):

	try:
		string.index("'")
		return True
	except ValueError:
		try:
			string.index("\\")
			return True
		except ValueError:
			return False


# adds data for word from one tweet
def add_word_data(word,s,w,k):

	# get the number of entries for the word
	sql_s = "SELECT * FROM corpus WHERE token='%s'" % word
	cur.execute(sql_s)

	r = cur.fetchone()		

	# check for bad search
	if (r == None):
		return

	# unpack data
	s_old = str(r[2])
	w_old = str(r[3])
	k_old = str(r[4])
	n = int(r[5])

	# saw one more observation
	if (s_old == ""):
		n = 1
		s_new = s
		w_new = w
		k_new = k
	else:
		n = n+1
		# now sum the other strings
		s_new = sql.sum_strings(s_old,s)
		w_new = sql.sum_strings(w_old,w)
		k_new = sql.sum_strings(k_old,k) 

	# now update the table
	sql_s = "UPDATE corpus SET s='%s', w='%s', k='%s', n='%s' WHERE token='%s'" % (s_new,w_new,k_new,n,word)

	cur.execute(sql_s)
	con.commit()


# adds data for state from one tweet
def add_state_data(state,s,w,k):

	if (state == "'"):
		return

	# get the number of entries for the word
	sql_s = "SELECT * FROM states WHERE id='%s'" % state
	
	cur.execute(sql_s)

	r = cur.fetchone()

	# check for bad search
	if (r == None):
		return

	# unpack data
	s_old = str(r[2])
	w_old = str(r[3])
	k_old = str(r[4])
	n = int(r[5])

	# saw one more observation

	if (s_old == ""):
		n = 1
		s_new = s
		w_new = w
		k_new = k
	else:
		n = n+1

		# now sum the other strings
		s_new = sql.sum_strings(s_old,s)
		w_new = sql.sum_strings(w_old,w)
		k_new = sql.sum_strings(k_old,k)

	# update the table
	sql_s = "UPDATE states SET s='%s', w='%s', k='%s', n='%s' WHERE id='%s'" % (s_new,w_new,k_new,n,state)

	cur.execute(sql_s)
	con.commit()

# load the modules that this script depends on
def load_depends():
        
        # changing globals
        global functional
        global text
        global sql

        # load them with imp
        text = imp.load_source("text","../PHelpers/text.py")
        sql = imp.load_source("sql","../PHelpers/sql.py")
        functional = imp.load_source("functional","../PHelpers/functional.py")	


# driver for going through the sql database
if __name__ == "__main__":

	# do imports
	load_depends()

	# connect to sql database
	con = sql.connect("WeatherKaggle")
	cur = con.cursor()

	# now go through the sql file and run above function on each line
	sql_s = "SELECT * FROM tweets"
	sql.apply_search(sum_data,sql_s,cur)

	cur.close()
	con.close()











