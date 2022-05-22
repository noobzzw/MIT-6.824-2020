package mr

import (
	"errors"
	"sync"
)

// 实现一个链表队列，用于存储失败的任务
type Node struct {
	data interface{}
	prev *Node
	next *Node
}

type LinkedQueue struct {
	head  *Node
	tail  *Node
	mutex *sync.Mutex
	count int
}

// 将元素添加至队列尾部
func (l *LinkedQueue) Push(data interface{}) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	newNode := Node{data: data, prev: l.tail.prev, next: l.tail}
	prev := l.tail.prev
	tail := l.tail
	prev.next = &newNode
	tail.prev = &newNode
	l.count++
}

// 将队列首元素出队
func (l *LinkedQueue) Poll() (interface{}, error) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	if l.count <= 0 {
		return nil, errors.New("linkedQueue is empty")
	}
	pollNode := l.head.next
	l.head.next = pollNode.next
	l.head.next.prev = l.head
	l.count--
	return pollNode.data, nil
}

func (l *LinkedQueue) Size() int {
	return l.count
}

func newLinkedQueue() LinkedQueue {
	queue := LinkedQueue{}
	head := Node{data: "head"}
	tail := Node{data: "tail"}
	queue.head = &head
	queue.tail = &tail
	queue.head.next = queue.tail
	queue.tail.prev = queue.head
	queue.head.prev = nil
	queue.tail.next = nil
	queue.count = 0
	queue.mutex = new(sync.Mutex)
	return queue
}
