package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var wg sync.WaitGroup
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	currentPendingTasks := make([]int, ntasks)
	for i := 0; i < ntasks; i++ {
		currentPendingTasks[i] = i
	}
	for {
		failedTask := make(chan int)
		newPendingTasks := make([]int, 0)
		rcvdResponse := make(chan struct{})
		go func(fT chan int) {
			for taskID := range failedTask {
				newPendingTasks = append(newPendingTasks, taskID)
			}
			rcvdResponse <- struct{}{}
		}(failedTask)

		for _, taskID := range currentPendingTasks {
			wkAddr := <-registerChan
			wg.Add(1)
			go func(wkAddr string, taskID int) {
				// Make RPC call
				fileName := ""
				if phase == mapPhase {
					fileName = mapFiles[taskID]
				}
				if success := call(wkAddr,
					"Worker.DoTask",
					DoTaskArgs{
						jobName,
						fileName,
						phase,
						taskID,
						n_other}, nil); !success {
					failedTask <- taskID
					wg.Done()
				} else {
					wg.Done()
					registerChan <- wkAddr
				}
			}(wkAddr, taskID)
		}
		wg.Wait()
		close(failedTask)
		<-rcvdResponse
		if len(newPendingTasks) == 0 {
			break
		}
		currentPendingTasks = newPendingTasks
	}
	fmt.Printf("Schedule: %v done\n", phase)
}
