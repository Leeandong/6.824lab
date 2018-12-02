package mapreduce

import (
	"encoding/json"
	"log"
	"os"
	"sort"
)

type ByDict []KeyValue

func (b ByDict) Len() int           { return len(b) }
func (b ByDict) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b ByDict) Less(i, j int) bool { return b[i].Key < b[j].Key }

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	var keyPair []KeyValue

	for m := 0; m < nMap; m++ {
		srcfileName := reduceName(jobName, m, reduceTask)
		srcfile, err := os.Open(srcfileName)
		if err != nil {
			log.Fatal(err)
		}

		dec := json.NewDecoder(srcfile)
		for dec.More() {
			var tmpKeyPair KeyValue
			err = dec.Decode(&tmpKeyPair)
			if err != nil {
				log.Fatal(err)
			}
			keyPair = append(keyPair, tmpKeyPair)
		}
	}

	sort.Sort(ByDict(keyPair))

	dstFile, err := os.OpenFile(outFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	enc := json.NewEncoder(dstFile)

	tmp := keyPair[0].Key
	var values []string
	for i := 0; i < len(keyPair); i++ {
		if keyPair[i].Key == tmp {
			values = append(values, keyPair[i].Value)
		} else {
			err = enc.Encode(KeyValue{tmp, reduceF(tmp, values)})
			if err != nil {
				log.Fatal(err)
			}
			values = nil
			values = append(values, keyPair[i].Value)
			tmp = keyPair[i].Key
		}
	}
	err = enc.Encode(KeyValue{tmp, reduceF(tmp, values)})
	if err != nil {
		log.Fatal(err)
	}

	err = dstFile.Close()
	if err != nil {
		log.Fatal(err)
	}

	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//
}
