



import imp

fout = None

def is_length(line):

	fout.write("0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0\n")

if __name__ == "__main__":

	# import functional
	functional = imp.load_source("functional","../PHelpers/functional.py")

	fout = open("out.csv","w")
	functional.apply(is_length,"temp.csv")



	
	
