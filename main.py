from MapReduce import FindCitations, FindCyclicReferences
import sys
import time

if __name__ == '__main__':

    quests = sys.argv
    if(len(quests) == 4):
        print(quests)
        option = str(sys.argv[1])
        num_workers = int(sys.argv[2])
        filename = str(sys.argv[3])
        start_time = time.time()
        if(option == "COUNT"):
            mr = FindCitations(num_workers)
            mr.start(filename)
        elif(option == "CYCLE"):
            mr = FindCyclicReferences(num_workers)
            mr.start(filename)
        print("--- %s seconds ---" % round((time.time() - start_time), 4))