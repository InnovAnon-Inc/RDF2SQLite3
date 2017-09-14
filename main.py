#! /usr/bin/env python

from getopt import getopt, GetoptError
from pandas import read_csv
import os
import sys
import sqlite3

from rdflib import Graph

def castForType (mapping, val, rowVal):
	if mapping[2] == "integer":
		print mapping, rowVal
		return int (val)
	if mapping[2] == "real":    return float (val)
	if mapping[2] == "text":    return str (val)
	raise Exception ()

def getDefaultForType (mapping):
	if mapping[2] == "integer": return 0
	if mapping[2] == "real":    return 0.0
	if mapping[2] == "text":    return ""
	raise Exception ()
	return None

# TODO hashing offers superior amortized asymptotic complexity
#def getMapping (icol, mappings):
#	for ic, ocoln, ocolt in mappings:
#		#print icol, ic, ocoln, ocolt
#		if icol == ic: return ocoln
#	#raise Exception ()
#	return None

#def getMapping (rowVal, mappings):
#	for mapping in mappings:
def getMapping (rowVals, mapping):
	for rowVal in rowVals:
		if rowVal[1] == mapping[0]:
			return rowVal[0], mapping[1], castForType (mapping, rowVal[2], rowVal)
	#raise Exception ()
	return None, mapping[1], getDefaultForType (mapping)

def getMappings (rowVals, mappings):
#	return map (lambda rv: getMapping (rv, mappings), rowVals)
	return map (lambda m: getMapping (rowVals, m), mappings)

def io (inputfile, mapfile, outputfile):
	g = Graph ()
	g.load (inputfile)

	gRows = list ([(str (subj), str (pred), str (obj))
		for subj, pred, obj in g])
	#obj_set  = set (map (lambda t: t[2], gRows)) # cell values
	subj_set = set (map (lambda t: t[0], gRows)) # row  IDs
	#pred_set = set (map (lambda t: t[1], gRows)) # col  names

	if mapfile: mappings = list ([tuple (x) for x in read_csv (
		mapfile, header=None, delim_whitespace=True).to_records (
			index=False)])
	# TODO pred_set is an awful default value for the Sqlite column names
	else:
		pred_set = set (map (lambda t: t[1], gRows)) # col  names
		mappings = zip (pred_set, pred_set, ["text"] * len (pred_set))

	#pred_set = pred_set.intersection (map (lambda t: t[0], mappings))

	con = sqlite3.connect (outputfile)
	with con:
		cur = con.cursor ()
		cols = map (lambda t: (t[1], t[2]), mappings)
		schema = ','.join (["%s %s" % (colName, colType)
			for colName, colType in cols])
		colNames  = ','.join (zip (*cols)[0])
		wildcards = ','.join ('?' for _ in cols)
		print schema
		cur.execute ("CREATE TABLE IF NOT EXISTS inspections(%s)" % schema)
		table_insert_string = "INSERT INTO inspections(%s) VALUES (%s)" % (colNames, wildcards)

		for rowID in subj_set:
			#rowVals = filter (lambda t: t[0] == rowID and t[1] in pred_set, gRows)
			rowVals = filter (lambda t: t[0] == rowID and t[1] in map (lambda t: t[0], mappings), gRows)
			#pred_objs = map (lambda t: (t[1], t[2]), rowVals)
			# TODO ensure correct order
			#preds = map (lambda t: (getMapping (t[0], mappings), t[1]),
			#	pred_objs)
			#preds = filter (lambda d: d, preds)
			#preds = map (lambda mapping: filter (lambda t: t[0] == mapping[1], pred_objs), mappings)
			# TODO wtf
			#print preds
			#print pred_objs
			#print rowVals
			#for rowVal in rowVals:
			#	for mapping in mappings:
			#		if rowVal[1] == mapping[0]:
			#			rowVal[0], mapping[0], rowVal[2]
			preds = getMappings (rowVals, mappings)
			preds = map (lambda t: t[2], preds)
			print preds
						
			cur.execute (table_insert_string, preds)
		#cur.executeMany (table_insert_string, data)
		con.commit ()

def getUsage (argv):
	usages = ["%s -i <inputfile.rdf> [-c <map.cnf>] -o <outputfile.sql>" % argv[0],
	          "%s -i <inputfile.rdf> [-c <map.cnf>] [-e <of_ext>]"       % argv[0]]
	return '\n'.join (usages)

def printUsage (argv): print getUsage (argv)

def parseArgs (argv):
	inputfile, mapfile, outputfile = None, None, None
	ofnameext = "sql"
	mapfilenameext = "cnf"
	try: opts, args = getopt (argv[1:], "hi:o:e:c:",
		["help", "if=", "of=", "ofe=", "conf="])
	except GetoptError:
		printUsage (argv)
		raise Exception ()
	for opt, arg in opts:
		if opt in ("-h", "--help"):
			printUsage (argv)
			return # None
		if   opt in ("-i", "--if"):   inputfile  = arg
		elif opt in ("-o", "--of"):   outputfile = arg
		elif opt in ("-e", "--ofe"):  ofnameext  = arg
		elif opt in ("-c", "--conf"): mapfile    = arg
		else: assert false
	if not inputfile: raise Exception ()
	ifname, ifnameext = os.path.splitext (inputfile)
	if not outputfile:
		outputfile = "%s.%s" % (ifname, ofnameext)
	if not mapfile:
		mapfilename = "%s.%s" % (ifname, mapfilenameext)
		if os.path.isfile (mapfilename): mapfile = mapfilename
	return inputfile, mapfile, outputfile

def main (argv):
	tmp = parseArgs (argv)
	if not tmp: return
	inputfile, mapfile, outputfile = tmp
	io (inputfile, mapfile, outputfile)

if __name__ == '__main__':
	main (sys.argv)
	sys.exit ()
