'''
Created on Nov 12, 2012

@author: Michael Huelfenhaus
'''

import csv
import fnmatch
import os


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

def store_results(week,results,path,metric_name,nodetype):
    with open(path, 'ab') as csvfile:
        result_writer = csv.writer(csvfile, delimiter='\t')
        print results
        for community, value in results:
            result_writer.writerow([week,metric_name,community,nodetype,value])
        

def calc_gini_from_dist_files(folder):
    '''
    Calculate the gini coefficients for all file with distributions in a folder 
    param folder:
    '''
    result_path = "../results/gini/gini.tsv"
    # remove old result file
    os.remove(result_path)
    
    # find files recursively
    matches = []
    for root, dirnames, filenames in os.walk('../results/degreedist'):
        for filename in fnmatch.filter(filenames, 'part*'):
            matches.append(os.path.join(root, filename))          
    
    for path in matches:
        week,results,nodetype = calc_gini_from_dist_file(path)
        # write results to tsv file
        store_results(week,results,result_path,'gini',nodetype)
    
def calc_gini_from_dist_file(path):
    '''
    Calculate the gini coefficients for all distributions in a file 
    @param path: path to file with distribution
    @type: C{Str}
    '''
    degree_dist_file = csv.reader(open(path), delimiter='\t')
    
    # values of the degree dist
    values = []
        
    gini_results = []
    # read first entry from file
    line = degree_dist_file.next()
    week = line[0]
    last_comm = line[1]
    values.append(int(line[3]))
    for line in degree_dist_file:
            if line[1] != last_comm:
                # new community found 
                # calc gini
                gini_results.append((last_comm,gini_coef(values)))
                # reset values
                last_comm = line[1]
                values = []
    
            # collect values of the degree dist
            values.append(int(line[3]))
    # calc gini for last
    gini_results.append((last_comm,gini_coef(values)))
            
            
    print path.split('-')
    # get node types  from file name
    nodetype = path.split('-')[1] + '-' + path.split('-')[2]
    return week,gini_results,nodetype

calc_gini_from_dist_files("")
