package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// import (
// 	"encoding/json"
// 	"fmt"
// 	"hash/fnv"
// 	"io"
// 	"log"
// 	"net/rpc"
// 	"os"
// 	"time"
// )

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func getCurrentDir() string {
	dir, err := os.Getwd()
	if err != nil {
		return "unknown"
	}
	return dir
}

func domap(mapf func(string, string) []KeyValue, taskId int, inputFileName string, nReduce int) error {
	// 这里只用了一个intermidiate存储中间keyvalue，最后按key排序，输出
	// 目标：每个key要使用ihash进行选择，先排序，然后遍历intermidiate，对每个kv进行ihash % nReduce，写入到中间文件中
	intermediate := []KeyValue{}
	file, err := os.Open(inputFileName)
	if err != nil {
		log.Fatalf("cannot open %v", inputFileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", inputFileName)

	}
	file.Close()

	fmt.Println("InputFileName: ", inputFileName)

	kva := mapf(inputFileName, string(content))

	intermediate = append(intermediate, kva...)
	fmt.Println("len(intermediate): ", len(intermediate))

	sort.Sort(ByKey(intermediate))

	// 写入到中间文件中文件中，对其中的每个kv都要ihash % nReduce
	// 输出文件命名：mr-X-Y
	for _, kv := range intermediate {
		target_reduce_id := ihash(kv.Key) % nReduce

		outputFileName := fmt.Sprintf("mr-%d-%d", taskId, target_reduce_id)
		outputFile, err := os.OpenFile(
			outputFileName,
			os.O_APPEND|os.O_CREATE|os.O_WRONLY,
			0644,
		)
		if err != nil {
			log.Fatalf("Cannot open file %v: %v", outputFileName, err)
		}

		// 将每一个kv编码后输入对应文件
		enc := json.NewEncoder(outputFile)
		err = enc.Encode(&kv)
		if err != nil {
			log.Fatalf("Failed to write: %v", err)
		}
		outputFile.Close() // 及时关闭文件
	}
	return nil

}

func doreduce(reducef func(string, []string) string, taskId int, nMap int) error {
	// 读取mr-X-taskId中的内容
	intermediate := []KeyValue{}
	for i := 0; i < nMap; i++ {
		inputFileName := fmt.Sprintf("mr-%d-%d", i, taskId)
		fmt.Printf("尝试打开文件: %s (当前目录: %s)\n", inputFileName, getCurrentDir())

		// inputFile, _ := os.Create(inputFileName)
		inputFile, err := os.Open(inputFileName)
		if err != nil {

			log.Printf("Cannot open file %v: %v，跳过此文件", inputFileName, err)
			continue
		}
		dec := json.NewDecoder(inputFile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	fmt.Println("len(intermediate): ", len(intermediate))

	sort.Sort(ByKey(intermediate))

	// 完成reduce，输出文件mr-out-taskId
	outputFileName := fmt.Sprintf("mr-out-%d", taskId)

	fmt.Printf("尝试打开文件: %s (当前目录: %s)\n", outputFileName, getCurrentDir())

	outfile, _ := os.Create(outputFileName)

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		output := reducef(intermediate[i].Key, values)

		fmt.Fprintf(outfile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	outfile.Close()
	return nil
}

func reportFinish(taskType string, taskId int) bool {
	// 请求参数
	args := FinishArgs{
		TaskType: taskType,
		TaskId:   taskId,
		EndTime:  time.Now(),
	}
	// 响应参数
	reply := FinishReply{}

	return call("Coordinator.Finish", &args, &reply)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		// 向coordinator请求任务
		args := Args{}
		reply := Reply{}

		log.Println("Request a task from coordinator...")

		ok := call("Coordinator.Allocate", &args, &reply)

		if !ok { // 有错误，退出
			return
		}
		log.Println("Coordinator.Allocate complete...")

		if reply.Task != nil {
			log.Printf("reply content: %+v, Task: %+v", reply, *reply.Task)
		} else {
			log.Printf("reply content: %+v, Task: nil", reply)
		}

		switch reply.Task.Type {
		case "map":
			log.Println("Worker do map task...")
			err := domap(mapf, reply.Task.Id, reply.Task.FileName, reply.NReduce) // mapf、task_id、输入文件路径、输出文件路径
			if err == nil {
				log.Println("map task complete...")
				reportFinish("map", reply.Task.Id)
			}
		case "reduce":
			log.Println("Worker do reduce task...")

			err := doreduce(reducef, reply.Task.Id, reply.NMap) // mapf、task_id、输入文件路径、输出文件路径
			if err == nil {
				log.Println("reduce task complete...")
				reportFinish("reduce", reply.Task.Id)

			}
		case "wait":
			log.Println("Worker waits for one second...")

			time.Sleep(time.Second)
		case "exit":
			log.Println("Worker exit...")
			return
		}

	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
