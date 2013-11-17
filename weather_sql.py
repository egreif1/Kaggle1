# SQL module for interacting with the WeatherKaggle database
# Assumes that you've already connected to the database
# and can pass in a cursor and connection


# gets a particular word from corpus
def get_tweet(word,con,cur):

	if (word == ""):
		return "" 

	word = con.escape_string(word)

	# now search corpus table for it
	sql_s = "SELECT * FROM corpus WHERE token='%s'" % word

	# get result
	cur.execute(sql_s)

	result = cur.fetchone()
	if (result == None):
		return ""

	else:
		return result















