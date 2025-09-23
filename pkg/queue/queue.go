package queue

type Queue interface {
	Enqueue(job *Job) error
	Dequeue() (*Job, error)
	GetJob(id string) (*Job, error)
	UpdateJob(job *Job) error
}
