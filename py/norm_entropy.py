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


# def store_results(week, results, path, metric_name, nodetype):
#    with open(path, 'ab') as csvfile:
#        result_writer = csv.writer(csvfile, delimiter='\t')
#        print results
#        for community, value in results:
#            result_writer.writerow([week, metric_name, community, nodetype, value])
        

def calc_entr_from_degree_files(folder, result_folder):
    '''
    Calculate the normalized entropy for all files with degrees in a folder 
    param folder:
    '''
    result_path = result_folder + "/norm_entropy.csv"
    # remove old result file
    try:
        os.remove(result_path)
    except:
        pass
    
    # find files recursively
    matches = []
    # for root, dirnames, filenames in os.walk('../results/degrees'):
    for root, dirnames, filenames in os.walk(folder):
        for filename in fnmatch.filter(filenames, 'part*'):
            matches.append(os.path.join(root, filename))
    week_files = {}
    
    for match in matches:
        directory = '/'.join(match.split('/')[:-1])
        part_file = match.split('/')[-1]
        if directory in week_files:
            week_files[directory].append(part_file)
        else:
            week_files[directory] = [part_file, ]
     
    degree_values = []
    # read file to get degree distribution for a week    
    
    for directory in week_files.keys():
        degrees = []
        
        # collect data of all part file for the same week
        for part in week_files[directory]:
            degree_file = csv.reader(open(directory + '/' + part), delimiter='\t')
            for line in degree_file:
                # list of community and degree
                degrees.append([line[3], int(line[2])])
            
        # parse directory name for nodetype and week
        nodetype = directory.split('-')[1] + '-' + directory.split('-')[2]
        week = directory.split('-')[-1]
        # add if degree_dist non empty
        if degrees != []:
            degree_values.append([degrees, week, nodetype])
        else:
            print 'empty', week, nodetype
    
    all_results = []
    for degrees, week, nodetype in degree_values:
        results = calc_entropy_from_degree_file(degrees)
        # create result list to do sorting
        all_results.extend([(week, 'norm_entropy', community, nodetype, value) for community, value in results])
        
    # sort results by week
    all_results = sorted(all_results, key=operator.itemgetter(3, 0))
    # write results to tsv file    
    with open(result_path, 'wb') as csvfile:
        result_writer = csv.writer(csvfile, delimiter='\t')
        for result in all_results:
            result_writer.writerow(result)
    
#        for community, value in results:
#            result_writer.writerow([week, metric_name, community, nodetype, value])
    
    
def calc_entropy_from_degree_file(degreedist):
    '''
    Calculate the gini coefficients for all distributions in a file 
    @param degreedist: list representing the degree distribution items [community,degree count]
    @type: C{List of List[str,int]}
    '''
    # degree_dist_file = csv.reader(open(path), delimiter='\t')
    
    # values of the degree dist
    values = []
        
    entropy_results = []
        
    # use first entry
    last_comm = degreedist[0][0]
    values.append(degreedist[0][1])
    for community, degreecount in degreedist[1:]:
            if community != last_comm:
                # new community found 
                # calc gini
                entropy_results.append((last_comm, norm_entropy(values)))
                # reset values
                last_comm = community
                values = []
    
            # collect values of the degree dist for this community
            values.append(degreecount)
            
    
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
