package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"sync"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//var buckets = map[int][]KeyValue{}
//var isAggregate = false

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	//go func() {
	//	for {
	//		time.Sleep(time.Second)
	//		exit := call("Coordinator.PseudoTask", nil, nil)
	//		if !exit {
	//			// 随着master退出
	//			//os.Exit(1)
	//			return
	//		}
	//	}
	//}()
RETRY:
	// Your worker implementation here.
	ok := callAllMapFinished()
	if !ok {
		fmt.Println("map start")
		mapId, NReduce, _ := callGetWorker(0)
		fmt.Println("63 mapId:", mapId)
		if mapId == -1 {
			time.Sleep(5 * time.Second)
			goto RETRY
		}

		// 心跳检测
		//stopCh := make(chan struct{})
		retryCh := make(chan int, 1)
		// 定时任务 设置超时时间可以由其他的Rpc 携带设置
		go func(retryCh chan int, mapId int) {
			ticker := time.NewTicker(500 * time.Millisecond)
			defer ticker.Stop()
			for range ticker.C {
				// 定时执行的代码
				fmt.Println("timeticker", mapId)
				isExit := callSetHeartTime(mapId, 0)
				if isExit {
					fmt.Fprintf(os.Stderr, "heartTest falied, reassigning tasks to other worker\n")
					//close(retryCh)
					retryCh <- 1
					return
				}
			}
		}(retryCh, mapId)

		// 暂时设置一个worker只能做固定的files 没有backup
		//fmt.Println("mapId:", mapId)
		assignments, _ := callGetAssignment(mapId)
		fmt.Println(assignments, mapId)

		if len(assignments) == 0 {
			fmt.Println("no files can read")
			callReturnMapId(mapId)
			goto RETRY
		}
		buckets := make([][]KeyValue, NReduce)
		intermediate := []KeyValue{}

		// 把files 转为intermediate
		for _, assignment := range assignments {
			file, err := os.Open(assignment)
			if err != nil {
				log.Fatalf("cannot open %v", assignment)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", assignment)
			}
			file.Close()
			kva := mapf(assignment, string(content))
			intermediate = append(intermediate, kva...)
		}
		fmt.Println("118 mapf pass:", mapId)
		// 把intermediate 放到 桶里面
		for _, kv := range intermediate {
			idx := ihash(kv.Key) % NReduce
			buckets[idx] = append(buckets[idx], kv)
		}

		// 在一次性写入文件中
		var wg sync.WaitGroup
		for i := 0; i < NReduce; i++ {
			idx := i
			kvs := buckets[idx]
			wg.Add(1)
			go func(reduceId int, kvs []KeyValue) {
				defer wg.Done()
				filename := fmt.Sprintf("mr-%d-%d", mapId, reduceId)
				// 创建一个临时文件
				tempName := fmt.Sprintf("tempfile-%d", reduceId)
				tempFile, err := ioutil.TempFile("", tempName)
				if err != nil {
					fmt.Println("Error creating temporary file:", err)
					return
				}
				defer tempFile.Close()
				enc := json.NewEncoder(tempFile)
				//kvs := buckets[reduceId]
				for _, kv := range kvs {
					err = enc.Encode(&kv)
					if err != nil {
						fmt.Println("Encode failed, err: ", err)
					}
				}
				//fmt.Println(tempFile)
				os.Rename(tempFile.Name(), filename)
			}(idx, kvs)
		}
		wg.Wait()

		time.Sleep(time.Second)
		if len(retryCh) != 0 {
			//close(stopCh)
			goto RETRY
		}
		// 如果到这里程序还没终止，则map成功
		callMapAssignFinished(len(assignments), mapId)
		time.Sleep(time.Second)
		fmt.Println("165 map success: ", mapId)

		// 重新开启一个map 或者 reduce
		// 关闭定时任务
		//close(stopCh)
		goto RETRY
	}
	fmt.Println("map finished...")

RETRY_REDUCE:
	fmt.Println("reduce start..")
	//exit := callPseudoTask()
	//if exit {
	//	// 随着master退出
	//	fmt.Println("worker exit..")
	//	os.Exit(1)
	//	//return
	//}
	finished := callAllReduceFinished()

	if finished {
		callIsDone()
		return
	}
	// reducewoker
	reduceId, mapNum, _ := callGetWorker(1)
	//fmt.Printf("166 reduceId:%d mapNum:%d\n", reduceId, mapNum)
	if reduceId == -1 {
		time.Sleep(5 * time.Second)
		goto RETRY_REDUCE
	}
	// 心跳检测
	// 定时任务 设置超时时间可以由其他的Rpc 携带设置
	retryCh := make(chan int, 1)
	// 定时任务 设置超时时间可以由其他的Rpc 携带设置
	go func(retryCh chan int, mapId int) {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for range ticker.C {
			// 定时执行的代码
			fmt.Println("timeticker", reduceId)
			isExit := callSetHeartTime(reduceId, 1)
			if isExit {
				fmt.Fprintf(os.Stderr, "heartTest falied, reassigning tasks to other worker\n")
				//close(retryCh)
				retryCh <- 1
				return
			}
		}
	}(retryCh, reduceId)

	//callGetmapIdUesd()
	var wg sync.WaitGroup
	var mu sync.Mutex
	var intermediate []KeyValue
	for i := 0; i < mapNum; i++ {
		wg.Add(1)
		go func(mapId int) {
			defer wg.Done()
			temp := callGetReduceAssignment(mapId, reduceId)
			mu.Lock()
			intermediate = append(intermediate, temp...)
			mu.Unlock()
		}(i)
	}
	fmt.Println("218 getReduce pass:", reduceId)

	wg.Wait()
	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%d", reduceId)
	tempName := fmt.Sprintf("tempfile-%d", reduceId)
	tempFile, err := ioutil.TempFile("", tempName)
	if err != nil {
		fmt.Println("Error creating temporary file:", err)
		return
	}
	defer tempFile.Close()
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
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	fmt.Println("246 reducef pass: ", reduceId)

	os.Rename(tempFile.Name(), oname)

	time.Sleep(time.Second)
	if len(retryCh) != 0 {
		goto RETRY_REDUCE
	}
	callReduceOutFinished(reduceId)
	time.Sleep(time.Second)

	goto RETRY_REDUCE
	// uncomment to send the Example RPC to the coordinator.
	//CallExample()
}
func callPseudoTask() bool {
	args := RpcArgs{}
	reply := RpcReply{}
	exit := call("Coordinator.IsDone", &args, &reply)
	return !exit
}

func callIsDone() {
	args := RpcArgs{}
	reply := RpcReply{}
	call("Coordinator.IsDone", &args, &reply)
	return
}

func callAllReduceFinished() bool {
	args := RpcArgs{}
	reply := RpcReply{}
	call("Coordinator.AllReduceFinished", &args, &reply)
	return reply.AllOutFinished
}

func callReduceOutFinished(reduceId int) {
	args := RpcArgs{}
	args.ReduceId = reduceId
	//args.ReduceCnt = reduceCnt
	reply := RpcReply{}
	call("Coordinator.ReduceOutFinished", &args, &reply)
	return
}
func callReturnMapId(mapId int) {
	args := RpcArgs{}
	args.WorkerId = mapId
	reply := RpcReply{}
	call("Coordinator.ReturnMapId", &args, &reply)
	return
}

//func callHeartDetection(mapId int) bool {
//	args := RpcArgs{}
//	args.WorkerId = mapId
//	reply := RpcReply{}
//	call("Coordinator.HeartDetection", &args, &reply)
//	return reply.HeartDetection
//}
//
func callSetHeartTime(workId int, workerType int) bool {
	args := RpcArgs{}
	args.WorkerType = workerType
	args.WorkerId = workId
	args.CurTime = time.Now()
	reply := RpcReply{}
	call("Coordinator.SetHeartTime", &args, &reply)
	return reply.IsExit
}

func callMapAssignFinished(assignments int, mapId int) {
	args := RpcArgs{}
	args.WorkerId = mapId
	args.AssignmentCnt = assignments
	reply := RpcReply{}
	call("Coordinator.MapAssignFinished", &args, &reply)
	return
}

func callAllMapFinished() bool {
	args := RpcArgs{}
	reply := RpcReply{}
	call("Coordinator.AllMapFinished", &args, &reply)
	return reply.FilesAllFinished
}

func callGetWorker(workerType int) (int, int, int) {
	args := RpcArgs{}
	args.CurTime = time.Now()
	args.WorkerType = workerType
	reply := RpcReply{}
	call("Coordinator.GetWorker", &args, &reply)
	if workerType == 0 {
		return reply.WorkerId, reply.NReduce, 0
	}
	//fmt.Println("315 MapIntermediateNum:", reply.MapIntermediateNum)
	return reply.WorkerId, reply.MapIntermediateNum, 0
}

func callGetReduceAssignment(mapId int, reduceId int) []KeyValue {
	args := RpcArgs{}
	args.WorkerType = 1
	args.WorkerId = mapId
	args.ReduceId = reduceId
	reply := RpcReply{}
	call("Coordinator.GetReduceAssignment", &args, &reply)
	return reply.ReduceAssignments
}

func callGetAssignment(mapId int) ([]string, bool) {
	args := RpcArgs{}
	args.WorkerId = mapId
	reply := RpcReply{}
	call("Coordinator.GetAssignment", &args, &reply)
	return reply.Assignments, reply.IsExit
}

//func emit(intermediate []*KeyValue) bool {
//	filename := fmt.Sprintf("rm-%d-%d", 0, ihash())
//	file, _ := os.Create(filename)
//	enc := json.NewEncoder(file)
//	for _, kv := range intermediate {
//		err := enc.Encode(&kv)
//		if err != nil {
//			fmt.Println("Encode failed, err: ", err)
//			return false
//		}
//	}
//	return true
//}

//func callWriteLocal(intermediate []KeyValue) {
//	args := RpcArgs{}
//	args.intermediate = intermediate
//	reply := RpcReply{}
//	call("Coordinator.WriteLocal", &args, &reply)
//
//}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
