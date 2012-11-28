'''
Created on Nov 12, 2012

@author: Michael Huelfenhaus
'''

import csv
import fnmatch
import os
import sys
from numpy import log

from getopt import getopt, GetoptError
import operator

help_message = 'python norm_entropy.py <degreedist_folder> <result_folder>'

def norm_entropy(degrees):
    '''
    Calculate the normalized entropy for a list of degrees
    @param degrees: list of degrees
    @type degrees: C{[int/double]}
    '''
   
    vertices = len(degrees) * 1.0
    edges = sum(degrees) * 1.0
    
    sum_part = 0.0
    for degree in degrees:
        deg_by_edge = degree / (2.0 * edges)
        sum_part += -1.0 * deg_by_edge * log(deg_by_edge)
        
        
    entropy = 1 / log(vertices) * sum_part
    
    return entropy


def calc_entr_from_degree_files(folder, result_folder):
    '''
    Calculate the normalized entropy for all files with degrees in a folder 
    param folder:
    '''
    
    # remove old result file
    result_path = result_folder + "/norm_entropy.csv"
    open(result_path, 'wb').close()
    
    # find result files recursively
    degree_files = []
    for root, dirnames, filenames in os.walk(folder):
        for filename in fnmatch.filter(filenames, 'part*'):
            degree_files.append(os.path.join(root, filename))
    # save all results for later sorting before saving
    all_results = []

    # calc norm entropy for all files
    for degree_file in degree_files:
        # parse file path name for nodetype and week
        nodetype = degree_file.split('-')[1] + '-' + degree_file.split('-')[2]
        week = degree_file.split('/')[-2].split('-')[-1]
        print degree_files.index(degree_file),nodetype, week 

        results = calc_entropy_from_degree_file(degree_file)
        
        all_results.extend([(week, 'norm_entropy', community, nodetype, value) for community, value in results])

    
    # sort results by week
    all_results = sorted(all_results, key=operator.itemgetter(3, 0))
    # write results to tsv file    
    with open(result_path, 'ab') as csvfile:
        result_writer = csv.writer(csvfile, delimiter='\t')
        for result in all_results:
            result_writer.writerow(result)
        
    
def calc_entropy_from_degree_file(degree_file):
    '''
    Calculate the gini coefficients for all distributions in a file 
    @param degrees: list representing the degrees in the graph [community,degree]
    @type: C{List of List[str,int]}
    '''
    degree_file_reader = csv.reader(open(degree_file), delimiter='\t')
    
    # values of the degree dist
    values = []
    # results for all communities
    entropy_results = []
    # read first line
    line = degree_file_reader.next()
    last_comm = line[3]
    values.append( int(line[2]))
    
    # parse line of degree file 
    for line in degree_file_reader:

        # list of community and degree
        community =line[3]
        degree =  int(line[2])

        # check if new community was found 
        if community != last_comm:
            # calc entropy
            entropy_results.append((last_comm, norm_entropy(values)))
            # reset values
            last_comm = community
            values = []
    
        # collect values of the degree dist for this community
        values.append(degree)
    
    # calc entropy with norm_entropy for last community
    entropy_results.append((last_comm, norm_entropy(values)))
    
    return entropy_results

if __name__ == "__main__":
    try:
        options, args = getopt(sys.argv[1:], "")
    except GetoptError:
        print >> sys.stderr, help_message
        sys.exit(2)
        
    if len(args) != 2:
        print >> sys.stderr, help_message
        sys.exit(2)

    source_folder = args[0]
    result_folder = args[1]    

    calc_entr_from_degree_files(source_folder, result_folder)

#dist = [1, 5, 7]
#print 'norm', norm_entropy(dist)
#print 'hand', 1.0 / log(3.0) * (-1.0 / 26.0 * log(1.0 / 26.0) - 5.0 / 26.0 * log(5.0 / 26.0) - 7.0 / 26.0 * log(7.0 / 26.0))
