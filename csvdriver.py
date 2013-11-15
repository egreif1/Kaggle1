# Goes through the training dataset
# and builds a corpus on non-trivial
# words which are saved in SQL database


# for importing other modules
import imp
import sys
import pymysql

# global dependencies
functional = None
text = None
sql = None

# global sql cursor
cur = None
con = None

# global variables
limit = 20

# load dependencies
def load_depends():
	
	# changing globals
	global functional
	global text
	global sql

	# load them with imp
	text = imp.load_source("text","../PHelpers/text.py")
	sql = imp.load_source("sql","../PHelpers/sql.py")
	functional = imp.load_source("functional","../PHelpers/functional.py")


# function to apply to each line of the training set to enter state values
def enter_state(line):


	# get the state
	state = line.split(",")[2]

	# clean the state
	state = text.clean_word(state)

	# remove punctuation
	state = text.strip_punc_all(state)

	# check if it's actually a state
	if (not(text.is_us_state(state))):
		return

	# insert into database, checking for duplicate error
	sql = "INSERT INTO states (state) VALUES ('"+state+"')"

	print sql

	try:
		cur.execute(sql)
		con.commit()
	except pymysql.IntegrityError:
		pass

# Returns the id of the state that is entered
def get_state(s):

	# search within the state database
	sql = "SELECT * FROM states WHERE state='"+pymysql.escape_string(s)+"'"

	# execute sql
	cur.execute(sql)
	result = cur.fetchone()
	
	if (result == None):
		return -1

	# get id from the result
	return result[0]

	
# function to apply to each line of the training set to enter
# the tweets
def enter_tweet(line):

	# get array specified by the line
	array = line.split("\",\"")

	# get values from array
	tid = int(text.remove_quotes(array[0]))
	tweet = pymysql.escape_string(text.remove_end_quotes(array[1]))

	# get the state id	
	state = get_state(text.strip_punc_all(array[2]))

	# create s, w, and k strings
	s = text.mysql_string_from_array(array[4:9])
	w = text.mysql_string_from_array(array[9:13])
	k = text.mysql_string_from_array(array[13:29])

	# check lengths	
	if (len(s) > 30 or len(w) > 30 or len(k) > 50 or len(tweet) > 200):
		return

	# insert everything into the table
	sql = "INSERT INTO tweets (id,tweet,s,w,k,state) VALUES (" + \
							"'" + str(tid) + "'," + \
							"'" + tweet   + "'," + \
							"'" + s       + "'," + \
							"'" + w       + "'," + \
           						"'" + k       + "'," + \
							"'" + str(state)   + "')"

	# execute the sql
	cur.execute(sql)
	con.commit()



# function to apply to each line of the training set to enter corpus values
def enter_tokens(line):

	# get the tweet
	tweet = line.split(",")[1]

	# get words from tweet
	words = tweet.split(" ")

	# go through words and enter them
	for word in words:

		# strip whitespace
		word = word.strip()
	
		# check if it's an emoticon
		if (text.is_emoticon(word)):
			sql = "INSERT INTO corpus (token) VALUES ('"+word+"')"
			
			try:
				cur.execute(sql)
			except pymysql.IntegrityError:
				pass

			# commit changes
			con.commit()

		# clean the word
		word = text.clean_word(word)
		
		# check if the word is too long
		if (len(word) > limit):
			continue

		# check if word is trivial
		if (text.is_trivial(word)):
			continue

		# check if it's a word, if so, enter it
		if (text.is_word(word)):
			
			sql = "INSERT INTO corpus (token) VALUES ('"+word+"')"

			try:
				cur.execute(sql)
			except pymysql.IntegrityError:
				pass	

			# commit changes
			con.commit()


# main method to run through training set
if __name__ == "__main__":

	# load dependencies
	load_depends()

	# global variables that will be used
	global cur
	global con

	# check that file name was entered
	if (len(sys.argv) < 2):
		print "Enter file to read."
		sys.exit()	

	# load file
	fin = sys.argv[1]

	# connect to the database
	con = sql.connect("WeatherKaggle")

	# get global cursor
	cur = con.cursor()

	# run function on each line of file
	functional.apply(enter_tweet,fin)

	# disconnect from sql database
	cur.close()
	con.close()



