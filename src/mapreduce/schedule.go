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
	var nother int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		nother = nReduce
	case reducePhase:
		ntasks = nReduce
		nother = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nother)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	scheduleSample(jobName, mapFiles, ntasks, nother, phase, registerChan)
	fmt.Printf("Schedule: %v done\n", phase)

}

func scheduleSample(jobName string, mapFiles []string, ntasks int, nother int, phase jobPhase, servers chan string) {
	work := make(chan int, ntasks)
	done := make(chan bool)
	exit := make(chan bool)
	runTasks := func(srv string) {
		for taskID := range work {
			filename := ""
			if phase == mapPhase {
				filename = mapFiles[taskID]
			}
			if call(srv,
				"Worker.DoTask",
				DoTaskArgs{
					jobName,
					filename,
					phase,
					taskID,
					nother}, nil) {
				done <- true
			} else {
				work <- taskID
			}

		}
	}

	go func() {
		for {
			select {
			case srv := <-servers:
				go runTasks(srv)
			case <-exit:
				return
			}
		}
	}()

	// Queue all the tasks
	for i := 0; i < ntasks; i++ {
		work <- i
	}

	// wait for all tasks to finish
	for i := 0; i < ntasks; i++ {
		<-done
	}
	close(work)
	exit <- true
}

func scheduleSample1(jobName string, mapFiles []string, ntasks int, nother int, phase jobPhase, registerChan chan string) {
	var wg sync.WaitGroup
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
						nother}, nil); !success {
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

	// atlmbp174:main sgadia$ bash ./test-mr.sh

	// ==> Part I
	// ok  	mapreduce	3.645s

	// ==> Part II
	// Passed test

	// ==> Part III
	// ok  	mapreduce	13.069s

	// ==> Part IV
	// ok  	mapreduce	14.855s

	// ==> Part V (inverted index)
	// Passed test
}

func scheduleSample2(jobName string, mapFiles []string, ntasks int, nother int, phase jobPhase, registerChan chan string) {
	completedTask := make(chan struct{})
	pendingTask := make(chan int)
	done := make(chan struct{})
	allTaskDone := false
	count := 0

	// task 1 - create a goroutine to count completed task
	go func() {
		for {
			<-completedTask
			count++
			if count == ntasks {
				done <- struct{}{}
			}
		}
	}()

	go func() {
		for i := 0; i < ntasks; i++ {
			pendingTask <- i
		}
	}()

	for {
		select {
		case <-done:
			allTaskDone = true
		case taskID := <-pendingTask:
			wkAddr := <-registerChan
			go func(wkAddr string, taskID int) {
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
						nother}, nil); !success {
					pendingTask <- taskID
				} else {
					completedTask <- struct{}{}
					registerChan <- wkAddr
				}
			}(wkAddr, taskID)
		}
		if allTaskDone {
			break
		}
	}
	// close(pendingTask)
	// close(completedTask)
	// close(done)
	// // atlmbp174:main sgadia$ bash ./test-mr.sh
	// // ==> Part I
	// // ok  	mapreduce	3.355s

	// // ==> Part II
	// // Passed test

	// // ==> Part III
	// // ok  	mapreduce	13.187s

	// // ==> Part IV
	// // ok  	mapreduce	15.184s

	// // ==> Part V (inverted index)
	// // Passed test

}
