package mr

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	allAssignmentFinished bool
	originfileslen        int
	nReduce               int
	nMap                  int
	files                 []string
	//filesFinished         map[string]bool
	//buckets               [][]KeyValue
	//filesNeedDelete       []string

	// Worker 分配id 递增
	//mapId     int
	// 还是用数组实现比较好
	mapId     []int
	reduceId  []int
	cnt       int
	reduceCnt int

	// 聚合数据部分
	isAggregate bool

	// 結束
	isDone bool
	//exitSignal int
	//stopCh     chan struct{}

	// 需要检测心跳的id new
	//heartDetectIds map[int]bool

	worker map[int]*worker
	mu     sync.Mutex
}

type worker struct {
	workerType     int // 0 map 1 reduce
	mapFiles       []string
	prevHeatTime   time.Time
	heartDetection bool
}

//type reduceWorker struct {
//	reduceId []int
//	brokerId []int
//}
//type Assignment struct {
//}

// Your code here -- RPC handlers for the worker to call.
//func (c *Coordinator) HeartDetection(args *RpcArgs, reply *RpcReply) error {
//	workerId := args.WorkerId
//	reply.HeartDetection = c.worker[workerId].heartDetection
//	return nil
//}
func (c *Coordinator) ReturnMapId(args *RpcArgs, reply *RpcReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	mapId := args.WorkerId
	c.mapId = append(c.mapId, mapId)
	//c.mapIdUesd = c.mapIdUesd & (^(1 << mapId))
	return nil
}

func (c *Coordinator) SetHeartTime(args *RpcArgs, reply *RpcReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	workerId := args.WorkerId
	workerType := args.WorkerType
	if workerType == 0 {
		reply.IsExit = !c.worker[workerId<<1].heartDetection
		c.worker[workerId<<1].prevHeatTime = args.CurTime
	} else if workerType == 1 {
		reply.IsExit = !c.worker[workerId<<1|1].heartDetection
		c.worker[workerId<<1|1].prevHeatTime = args.CurTime
	}
	return nil
}

func (c *Coordinator) MapAssignFinished(args *RpcArgs, reply *RpcReply) error {
	c.mu.Lock()
	mapId := args.WorkerId
	mapId = mapId << 1
	if !c.worker[mapId].heartDetection {
		return nil
	}
	c.cnt += args.AssignmentCnt
	c.worker[mapId].heartDetection = false
	//c.heartDetectIds[mapId<<1] = false
	c.mu.Unlock()
	return nil
}

func (c *Coordinator) ReduceOutFinished(args *RpcArgs, reply *RpcReply) error {
	c.mu.Lock()
	reduceId := args.ReduceId
	reduceId = (reduceId << 1) | 1
	c.worker[reduceId].heartDetection = false
	c.reduceCnt += 1
	c.mu.Unlock()
	return nil
}

func (c *Coordinator) GetWorker(args *RpcArgs, reply *RpcReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// map
	if args.WorkerType == 0 {
		if len(c.mapId) == 0 {
			reply.WorkerId = -1
			//time.Sleep(2 * time.Second)
			return nil
		}
		mapId := c.mapId[0]
		c.mapId = c.mapId[1:]
		mapId = mapId << 1
		c.worker[mapId] = &worker{}
		c.worker[mapId].mapFiles = []string{}
		// 用于心跳检测
		c.worker[mapId].prevHeatTime = time.Now()
		c.worker[mapId].heartDetection = true
		//c.heartDetectIds[(mapId<<1)|0] = true
		c.worker[mapId].prevHeatTime = args.CurTime
		c.worker[mapId].workerType = 0
		reply.WorkerId = mapId >> 1
	} else if args.WorkerType == 1 {
		if len(c.reduceId) == 0 {
			reply.WorkerId = -1
			return nil
		}
		reduceId := c.reduceId[0]
		c.reduceId = c.reduceId[1:]
		reduceId = reduceId<<1 | 1
		// 初始化 worker
		c.worker[reduceId] = &worker{}
		// 用于心跳检测
		c.worker[reduceId].heartDetection = true
		c.worker[reduceId].prevHeatTime = args.CurTime
		c.worker[reduceId].workerType = 1
		reply.WorkerId = reduceId >> 1
		reply.MapIntermediateNum = c.nMap
		//reply.MapIdUsed = c.mapIdUesd
		//fmt.Println("204 MapIntermediateNum:", reply.MapIntermediateNum)
	}

	reply.NReduce = c.nReduce
	return nil
}

func (c *Coordinator) GetReduceAssignment(args *RpcArgs, reply *RpcReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	mapId, reduceId := args.WorkerId, args.ReduceId
	intermediateFile := fmt.Sprintf("mr-%d-%d", mapId, reduceId)
	file, err := os.Open(intermediateFile)
	//fmt.Printf("mapId: %d, reduceId: %d\n", mapId, reduceId)
	if err != nil {
		fmt.Println("171 open intermediateFile filed:", err)
		return nil
	}
	fmt.Println("222 file open", intermediateFile)
	dec := json.NewDecoder(file)
	kva := []KeyValue{}
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kva = append(kva, kv)
	}
	reply.ReduceAssignments = kva
	return nil
}

func (c *Coordinator) GetAssignment(args *RpcArgs, reply *RpcReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var assignments []string
	mapId := args.WorkerId
	mapId <<= 1
	if len(c.files) >= 3 {
		assignments = c.files[:3]
		c.files = c.files[3:]
		for _, assignment := range assignments {
			c.worker[mapId].mapFiles = append(c.worker[mapId].mapFiles, assignment)
		}
	} else if len(c.files) < 3 && len(c.files) > 0 {
		assignments = c.files
		c.files = []string{}
		for _, assignment := range assignments {
			c.worker[mapId].mapFiles = append(c.worker[mapId].mapFiles, assignment)
		}
	} else if len(c.files) == 0 {
		time.Sleep(time.Second)
		return nil
	}
	//reply.IsExit = !c.heartDetectIds[mapId<<1]
	reply.Assignments = assignments
	return nil
}

func (c *Coordinator) AllReduceFinished(args *RpcArgs, reply *RpcReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.reduceCnt == c.nReduce {
		reply.AllOutFinished = true
	}
	return nil
}

func (c *Coordinator) AllMapFinished(args *RpcArgs, reply *RpcReply) error {

	c.mu.Lock()
	//time.Sleep(10 * time.Second)
	defer c.mu.Unlock()
	if c.cnt == c.originfileslen {
		reply.FilesAllFinished = true
	}
	return nil
}

func (c *Coordinator) IsDone(args *RpcArgs, reply *RpcReply) error {
	c.mu.Lock()
	c.isDone = true
	c.mu.Unlock()
	return nil
}

func (c *Coordinator) PseudoTask(args *RpcArgs, reply *RpcReply) error {

	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false
	// Your code here.
	c.mu.Lock()
	if c.isDone {
		ret = true
	}
	//close(c.stopCh)
	//c.exitSignal = 1
	c.mu.Unlock()
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.
	c.nReduce = nReduce
	c.originfileslen = len(files)
	c.files = files
	c.worker = make(map[int]*worker)
	c.reduceCnt = 0
	//c.heartDetectIds = make(map[int]bool)
	// 限定死3個 分配任務其實也限定了
	c.mapId = []int{0, 1, 2}
	c.nMap = len(c.mapId)
	for idx := 0; idx < c.nMap; idx++ {
		c.worker[idx<<1] = &worker{}
		c.worker[idx<<1].heartDetection = false
	}

	for idx := 0; idx < nReduce; idx++ {
		c.reduceId = append(c.reduceId, idx)
		c.worker[idx<<1|1] = &worker{}
		c.worker[idx<<1|1].heartDetection = false
	}

	c.server()
	// 检测心跳
	go func() {
		for {
			c.mu.Lock()
			//fmt.Println("mapId", c.mapId, "now", time.Now())
			if c.isDone {
				break
			}
			for id, worker := range c.worker {
				//fmt.Println("workId", id>>1)
				if !worker.heartDetection {
					continue
				}
				if time.Now().Sub(worker.prevHeatTime) > 10*time.Second {
					fmt.Println(time.Now().Sub(worker.prevHeatTime))
					fmt.Println("workId", id>>1, "hearTime", worker.prevHeatTime, "now", time.Now())
					fmt.Println("workId", id>>1, "crash")
					if worker.workerType == 0 {
						c.files = append(c.files, worker.mapFiles...)
						c.mapId = append(c.mapId, id>>1)
						worker.heartDetection = false
					} else {
						c.reduceId = append(c.reduceId, id>>1)
						worker.heartDetection = false
					}
				}
			}
			c.mu.Unlock()
			time.Sleep(10 * time.Second)
		}
	}()
	//go func() {
	//	for {
	//		time.Sleep(10 * time.Second)
	//		var wg sync.WaitGroup
	//		c.mu.Lock()
	//		if c.isDone {
	//			c.mu.Unlock()
	//			return
	//		}
	//		//heartDetectIds := c.heartDetectIds
	//		//fmt.Println("362 heartDetectIds", &heartDetectIds, &c.heartDetectIds)
	//		var mu sync.Mutex
	//		for id, ok := range c.heartDetectIds {
	//			wg.Add(1)
	//			mu.Lock()
	//			tOk_, tId_ := ok, id
	//			mu.Unlock()
	//			go func(tOk bool, tId int) {
	//				defer wg.Done()
	//				if !tOk {
	//					return
	//				}
	//				type_ := tId & 1
	//				tId >>= 1
	//				if type_ == 0 {
	//					// map
	//					//c.mu.Lock()
	//					if time.Now().Sub(c.worker[tId].prevHeatTime) > 10 {
	//						c.worker[tId] = &worker{}
	//						files := c.worker[tId].mapFiles
	//						for _, file := range files {
	//							c.files = append(c.files, file)
	//						}
	//						//c.mapIdUesd = c.mapIdUesd & (^(1 << tId))
	//						c.mapId = append(c.mapId, tId)
	//						c.heartDetectIds[(tId<<1)|0] = false
	//					}
	//					//c.mu.Unlock()
	//				} else if type_ == 1 {
	//					// reduce
	//					//c.mu.Lock()
	//					if time.Now().Sub(c.worker[tId].prevHeatTime) > 10 {
	//						c.worker[tId] = &worker{}
	//						c.heartDetectIds[(tId<<1)|1] = false
	//						c.reduceId = append(c.reduceId, tId)
	//					}
	//					//c.mu.Unlock()
	//				}
	//			}(tOk_, tId_)
	//		}
	//		wg.Wait()
	//		c.mu.Unlock()
	//	}
	//}()

	//go func() {
	//	var wg sync.WaitGroup
	//	for {
	//		for i := 1; i <= 10; i++ {
	//			wg.Add(1)
	//			go func(workId int) {
	//				defer wg.Done()
	//				if c.worker[workId] == nil {
	//					return
	//				}
	//				if !c.worker[workId].idIsUesd {
	//					return
	//				}
	//				if time.Now().Sub(c.worker[workId].prevHeatTime) > 10 {
	//					// 回收资源
	//					files := c.worker[workId].filenames
	//					for _, file := range files {
	//						c.files = append(c.files, file)
	//					}
	//					for i := 0; i < nReduce; i++ {
	//						filename := fmt.Sprintf("rm-%d-%d", workId, i)
	//						c.filesNeedDelete = append(c.filesNeedDelete, filename)
	//					}
	//					intermediates := c.worker[workId].intermediate
	//					for _, intermediate := range intermediates {
	//						c.worker[workId].intermediate = append(c.worker[workId].intermediate, intermediate)
	//					}
	//
	//					c.wokerId = append(c.wokerId, workId)
	//					c.worker[workId].filenames = []string{}
	//					c.worker[workId].idIsUesd = false
	//					c.worker[workId].heartDetection = false
	//
	//					return
	//				}
	//				//c.worker[mapId].prevHeatTime = time.Now()
	//			}(i)
	//		}
	//		wg.Wait()
	//	}
	//}()

	// 删除写入的文件

	//go func() {
	//	for {
	//		if c.allAssignmentFinished {
	//			return
	//		}
	//		if len(c.filesNeedDelete) == 0 {
	//			continue
	//		}
	//		for _, filename := range c.filesNeedDelete {
	//			err := os.Remove(filename)
	//			if err != nil {
	//				fmt.Println("remove", filename, "err :", err)
	//				return
	//			}
	//		}
	//	}
	//}()
	return &c
}
