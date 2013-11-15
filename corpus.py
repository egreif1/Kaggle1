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


# function to apply to each line of the training set
def enter_line(line):

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
	functional.apply(enter_line,fin)

	# disconnect from sql database
	cur.close()
	con.close()



