package mr

import (
	"fmt"
	"testing"
)

func TestPoll(t *testing.T) {
	queue := newLinkedQueue()
	go func() {
		for {
			task := Task{}
			task.TaskIndex = 1
			queue.Push(task)
		}

	}()
	for {
		data, err := queue.Poll()
		if err == nil {
			fmt.Println(data)
		}
	}

}
