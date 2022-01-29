from abc import ABC, abstractmethod
from multiprocessing import Process
import zmq
import time
import os

# Emir Alaattin Yilmaz 2021

class MapReduce(ABC):
    def __init__(self, num_workers):
        self.num_workers = num_workers
        self.iter_no = 20
    
    @abstractmethod
    def Map(self, map_input):
        pass

    @abstractmethod
    def Reduce(self, reduce_input):
        pass

    def _Producer(self, producer_input : list):

        print ('producer process id:', os.getpid())
        context = zmq.Context()
        producer_sender = context.socket(zmq.PUSH)
        producer_sender.bind("tcp://127.0.0.1:5558")

        rough_chunk_size = len(producer_input) // self.num_workers
        remainder = len(producer_input) % self.num_workers
        rem = remainder

        for w in range(self.num_workers):
            chunk_size = rough_chunk_size
            if (rem > 0):
                chunk_size = rough_chunk_size + 1
                json_chunk = {'chunk_num': w, 'data' : producer_input[w*chunk_size:(w+1)*chunk_size]}
                rem -= 1
            else:
                json_chunk = {'chunk_num': w, 'data' : producer_input[remainder+w*chunk_size:remainder+(w+1)*chunk_size]}
            print("chunk number {} is sent".format(w))
            time.sleep(0.1)
            producer_sender.send_json(json_chunk)

    def _Consumer(self):

        pid = os.getpid()
        print ('consumer process pid:', pid)
        context = zmq.Context()

        consumer_receiver = context.socket(zmq.PULL)
        consumer_receiver.connect("tcp://127.0.0.1:5558")

        consumer_sender = context.socket(zmq.PUSH)
        consumer_sender.connect("tcp://127.0.0.1:5559")

        #taken_pieces = []

        for i in range (self.iter_no):
            print("consuming {} with pid = {}".format(i, pid))
            try:
                consumer_receiver.RCVTIMEO = 3000
                piece_json = consumer_receiver.recv_json()
                print("Taken chunk ",piece_json['chunk_num'], " by pid: ", pid)
                partial_result = self.Map(piece_json['data'])
                if(len(partial_result)):
                    consumer_sender.send_json(partial_result)
                if(len(piece_json)):
                    #print("BROKEN RETURN in ", i)
                    break
            except:
                print ("Trying to recieve message again!")

    def _ResultCollector(self):

        print ('resultcollector process id:', os.getpid())
        context = zmq.Context()
        result_collector_receiver = context.socket(zmq.PULL)
        result_collector_receiver.bind("tcp://127.0.0.1:5559")

        collected_results = []
        
        for i in range(self.num_workers):
            partial_result = result_collector_receiver.recv_json()
            collected_results.append(partial_result)

        reduced_result = self.Reduce(collected_results)
        
        write_file_name = 'results.txt'
        with open(write_file_name, 'w') as f:
            f.write(str(reduced_result))
            print("written to file: {}".format(write_file_name))

    def start(self, filename):

        # Reading the file
        data = []
        with open(filename, "r") as f:
            for line in f.read().split("\n"):
                nodes = line.split()
                if(len(nodes)):
                    data.append(nodes)
            f.close()

        # Creating processes
        producer_process = Process(target=self._Producer, args=(data,))
        consumer_processes = [ Process(target=self._Consumer) for i in range(self.num_workers) ]
        result_collector_process = Process(target=self._ResultCollector)

        # Start producer process
        producer_process.start()
        
        # Start result collector process
        result_collector_process.start()

        # Start Consumer processes
        for i in range(self.num_workers):
            consumer_processes[i].start()

        # Joining producer process
        producer_process.join()
        print("producer joined")

        # Joining result collector processes
        result_collector_process.join()
        print("result_collector joined")

        # Joining consumer processes
        for i in range(self.num_workers):
            consumer_processes[i].join()
            print("consumer {} joined".format(i))

        print("Finished!")


class FindCitations(MapReduce):
    def Map(self, nodes):
        citation_counts = dict()
    
        for n in nodes:

            if(n[1] not in citation_counts):
                citation_counts[n[1]] = 0

            if(n[1] in citation_counts):
                citation_counts[n[1]] += 1
            
        return citation_counts

    def Reduce(self,local_citation_counts_list):

        global_citation_counts = dict()

        for local_citation_counts in local_citation_counts_list:
            for paper_id, citation_count in local_citation_counts.items():
                if(paper_id not in global_citation_counts):
                    global_citation_counts[paper_id] = citation_count
                else:
                    global_citation_counts[paper_id] += citation_count
        
        return global_citation_counts

class FindCyclicReferences(MapReduce):

    def Map(self, nodes):

        citations = dict()

        for n in nodes:
            if(n[0] not in citations):
                citations[n[0]] = []
            
            citations[n[0]].append(n[1])
            
        return citations

    def Reduce(self,local_citations_list):

        global_citations = dict()
        cyclic_citations = dict()

        # Merging all the citation information into a global dictionary

        for local_citations in local_citations_list:
            for from_node in local_citations.keys():
                if(from_node not in global_citations):
                    global_citations[from_node] = local_citations[from_node]
                else:
                    global_citations[from_node] += local_citations[from_node]
        
        # Finding the cycles
        for from_node, to_nodes in global_citations.items():
            for tn in to_nodes:
                if(tn in global_citations and from_node in global_citations[tn]):
                    if(from_node < tn):
                        cyclic_citations['({}, {})'.format(from_node,tn)] = 1
                    else:
                        cyclic_citations['({}, {})'.format(tn,from_node)] = 1

        return cyclic_citations