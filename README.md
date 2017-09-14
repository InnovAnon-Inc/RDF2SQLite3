RDF/XML to SQLite3 DataBase conversion utility

==========

see *.cnf for example conversion configuration file

USAGE

./main.py -i <inputfile.rdf> [-c <map.cnf>] -o <outputfile.sql>

./main.py -i <inputfile.rdf> [-c <map.cnf>] [-e <of_ext>]

inputfile.rdf can be any input file with RDF format. filename and filename extension are irrelevant.

map.cnf is optional. default filename extension is .cnf. default filename is <inputfile.cnf>. notice that the input filename extension is dropped.

outputfile.sql is optional. default filename extension is .sql. default filename is <inputfile.sql>. notice that the input filename extension is dropped.
