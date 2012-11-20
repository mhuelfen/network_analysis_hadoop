'''
Created on Nov 12, 2012

@author: Michael Huelfenhaus
'''

import csv
import fnmatch
import os
import sys

from getopt import getopt, GetoptError
import operator

help_message = 'python gini_coef.py <degreedist_folder> <result_folder>'

def gini_coef(dist):
    '''
    Calculate the Gini coefficient of a ordered distribution
    @param dist: distribution for that the gini coefficient is calculated
    @type dist: C{[int/double]}
    '''
   
    n = len(dist)
    
    numerator = 0
    
    for i in range(1, n + 1):
        numerator += (n + 1.0 - i) * dist[i - 1]
    gini = 1.0 / n * (n + 1 - 2 * (numerator / sum(dist)))
    
    return gini

#def store_results(week, results, path, metric_name, nodetype):
#    with open(path, 'ab') as csvfile:
#        result_writer = csv.writer(csvfile, delimiter='\t')
#        print results
#        for community, value in results:
#            result_writer.writerow([week, metric_name, community, nodetype, value])
        

def calc_gini_from_dist_files(folder, result_folder):
    '''
    Calculate the gini coefficients for all file with distributions in a folder 
    param folder:
    '''
    # result_path = "../results/gini/gini.tsv"
    result_path = result_folder + "/gini.tsv"
    # remove old result file
    try:
        os.remove(result_path)
    except:
        pass
    
    # find files recursively
    matches = []
    # for root, dirnames, filenames in os.walk('../results/degreedist'):
    for root, dirnames, filenames in os.walk(folder):
        for filename in fnmatch.filter(filenames, 'part*'):
            matches.append(os.path.join(root, filename))
    week_files = {}
    
    for match in matches:
        dir = '/'.join(match.split('/')[:-1])
        part_file = match.split('/')[-1]
        if dir in week_files:
            week_files[dir].append(part_file)
        else:
            week_files[dir] = [part_file,]
     
    degree_dists = []
    # read file to get degree distribution for a week    
    
    for dir in week_files.keys():
        degreedist = []
        
        # collect data of all part file for the same week
        for part in week_files[dir]:
            degree_dist_file = csv.reader(open(dir + '/' + part), delimiter='\t')
            for line in degree_dist_file:
                # list of community and degreecount
                degreedist.append([line[1],int(line[3])])
            
        # sort to get degree distribution sort by community and degree
        degreedist = sorted(degreedist, key=operator.itemgetter(0,1))
        # parse dir name for nodetype and week
        nodetype = dir.split('-')[1] + '-' + dir.split('-')[2]
        week = dir.split('-')[-1]
        # add if degree_dist non empty
        if degreedist !=[]:
            degree_dists.append([degreedist,week,nodetype])
        else:
            print 'empty',week, nodetype
    
    all_results = []
    for degreedist,week,nodetype in degree_dists:
        results = calc_gini_from_dist_file(degreedist)
        # create result list to do sorting
        all_results.extend([(week, 'gini', community, nodetype, value) for community, value in results])
        
    # sort results by week
    all_results = sorted(all_results, key=operator.itemgetter(3,0))
    # write results to tsv file    
    with open(result_path, 'wb') as csvfile:
        result_writer = csv.writer(csvfile, delimiter='\t')
        for result in all_results:
            result_writer.writerow(result)
    
#        for community, value in results:
#            result_writer.writerow([week, metric_name, community, nodetype, value])
    
    
def calc_gini_from_dist_file(degreedist):
    '''
    Calculate the gini coefficients for all distributions in a file 
    @param degreedist: list representing the degree distribution items [community,degree count]
    @type: C{List of List[str,int]}
    '''
    #degree_dist_file = csv.reader(open(path), delimiter='\t')
    
    # values of the degree dist
    values = []
        
    gini_results = []
        
    # use first entry
    last_comm = degreedist[0][0]
    values.append(degreedist[0][1])
    for community,degreecount in degreedist[1:]:
            if community != last_comm:
                # new community found 
                # calc gini
                gini_results.append((last_comm, gini_coef(values)))
                # reset values
                last_comm = community
                values = []
    
            # collect values of the degree dist for this community
            values.append(degreecount)
            
    
    # calc gini with gini_coef for last community
    gini_results.append((last_comm, gini_coef(values)))
    
    return gini_results

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

    calc_gini_from_dist_files(source_folder, result_folder)
