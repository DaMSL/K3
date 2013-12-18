#! /usr/bin/python2

import random
import os
import argparse

def genCollections(numOfColls, filePath):
    wordsMap = {1:"{word=\"foo\"}", 2:"{word=\"bar\"}", 3:"{word=\"baz\"}"}    
    totalWords = 0
    fooCount = 0
    barCount = 0
    bazCount = 0
    with open(filePath, 'w') as f:
        for collIndex in range(numOfColls):            
            f.write("{CNS={}, ANS={}, DS=[")
            
            for recordIndex in range(random.randint(1,9)):
                randomNum = random.randint(1,3)            
                f.write(wordsMap.get(randomNum) + ", ")
                
                if randomNum == 1:
                    fooCount = fooCount + 1
                elif randomNum == 2:
                    barCount = barCount + 1
                else :
                    bazCount = bazCount + 1

            randomNum = random.randint(1,3)                                   
            f.write(wordsMap.get(randomNum) + "], Collection}\n")            
            if randomNum == 1:
                fooCount = fooCount + 1
            elif randomNum == 2:
                barCount = barCount + 1
            else :
                bazCount = bazCount + 1
    
    totalWords = fooCount + barCount + bazCount

    print "word    count"
    print "foo" + "     " + str(fooCount)
    print "bar" + "     " + str(barCount)
    print "baz" + "     " + str(bazCount)
    print ""
    print "total number of words: " + str(totalWords)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description = 'Collections generator for k3 words count')
    parser.add_argument('numOfColls', help = 'Total number of collections', type = int) 
    parser.add_argument('path', help = '/path/to/wordsCollection.txt', type = str)
    args = parser.parse_args()
    genCollections(args.numOfColls, args.path)        
