# Set of functions that actually classify the tweets
# based on different methods.

# dependence
import string
import sys
import weather_sql
import imp


# global variables
text = None
functional = None
sql = None
cur = None
con = None
initiated = 0
util = None
fout = None



# load the modules that this script depends on
# and connect to sql database
def initiate():
        
        # changing globals
        global functional
        global text
        global sql
	global con
	global cur
	global initiated
	global util

        # load them with imp
        text = imp.load_source("text","../PHelpers/text.py")
        sql = imp.load_source("sql","../PHelpers/sql.py")
        functional = imp.load_source("functional","../PHelpers/functional.py")	
	util = imp.load_source("util","../PHelpers/util.py")


	# connect to sql database
	con = sql.connect("WeatherKaggle")
	cur = con.cursor()
		
	# set flag
	initiated = 1

# basic decision tree classification of a tweet
def tree_one(line):

	if(not(initiated)):
		initiate()

	# unpack line
	l_array = line.split("\",\"")

	t_id = int(l_array[0][1:])
	tweet = l_array[1]

	# baseline response says remark is about today
	resp = [str(t_id),"0","0","0","0","0","1","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0"]


	# decision tree choice one
	if (util.has_word("yesterday",tweet)):
		set_response("w",4,resp)
	if (util.has_word("tomorrow",tweet)):
		set_response("w",2,resp)
	if (util.has_word("love",tweet)):
		set_response("s",4,resp)
	if (util.has_word("hate",tweet)):
		set_response("s",2,resp)

	fout.write(",".join(resp)+"\n")


# sets response string
def set_response(cat,num,resp):
	
	if (cat == "w"):
		for i in range(6,10):
			if (num + 5 == i):
				resp[i] = "1"
			else:
				resp[i] = "0"

	else:
		for i in range(1,5):
			if (num == i):
				resp[i] = "1"
			else:
				resp[i] = "0"


		




# naive bayes classification of a tweet
def naive_bayes(line):

	# check for initiation
	if (not(initiated)):
		initiate()

	# unpack line
	l_array = line.split("\",\"")
	t_id = int(l_array[0][1:])
	tweet = l_array[1]

	# initial classification strings
	s=""
	w=""
	k=""
	n = 1
	
	# make tweet into word list
	t_list = tweet.split(" ")
	
	# go through and get each word
	for word in t_list:

		if (not(text.is_emoticon(word))):
			word = text.strip_punc_all(word)

		# get the word's entry in corpus
		result = weather_sql.get_tweet(word,con,cur)

		# check for bad query
		if (result == ""):
			continue

		# unpack
		s_obs = result[2]
		w_obs = result[3]
		k_obs = result[4]
		n = int(result[5])

		# sum them
		s = util.sum_strings(s,util.divide_string(s_obs,n))
		w = util.sum_strings(w,util.divide_string(w_obs,n))
		k = util.sum_strings(k,util.divide_string(k_obs,n))
	
	# now predict
	s_array = s.split(",")
	w_array = w.split(",")
	k_array = k.split(",")

	s_pred = util.get_max_tuple(s_array)[0]
	w_pred = util.get_max_tuple(w_array)[0]

	# create the s,w strings
	s_ans = ""
	w_ans = ""
	for i in range(0,5):
		if (i == s_pred):
			s_ans = s_ans+"1,"
		else:
			s_ans = s_ans+"0,"
	s_ans = s_ans[0:-1]
	for i in range(0,4):
		if (i == w_pred):
			w_ans = w_ans+"1,"
		else:
			w_ans = w_ans+"0,"
	w_ans = w_ans[0:-1]

	if (k == ""):
		k = "0,0,0,0,0,0,0,0,0,0,0,0,0,0,0"

	

	# write out the prediction string
	prediction = str(t_id) + "," + s_ans + "," + w_ans + "," + k

	fout.write(prediction+"\n")
		












# main method for classifying whole file of tweets
if __name__ == "__main__":

	global fout

	# take in file to classify
	if (len(sys.argv) < 2):
		print("Enter file to classify.")
		sys.exit()
	
	# input file
	fin = sys.argv[1]

	# initiate the script	
	initiate()

	# output file
	fout = open("pred.csv","w")

	# use apply
	functional.apply(tree_one,fin)

	fout.close()
	con.close()
	cur.close()

	

























