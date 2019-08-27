package ravenworker

func MustWithRavenURL(s string) OptionFunc {
	fn, _ := WithRavenURL(s)
	return fn
}

func MustWithFlowID(flowID string) OptionFunc {
	fn, _ := WithFlowID(flowID)
	return fn
}

func MustWithWorkerID(workerID string) OptionFunc {
	fn, _ := WithWorkerID(workerID)
	return fn
}
