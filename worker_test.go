package ravenworker

func MustWithRavenURL(s string) OptionFunc {
	fn, err := WithRavenURL(s)
	if err != nil {
		panic(err)
	}
	return fn
}

func MustWithFlowID(flowID string) OptionFunc {
	fn, err := WithFlowID(flowID)
	if err != nil {
		panic(err)
	}
	return fn
}

func MustWithWorkerID(workerID string) OptionFunc {
	fn, err := WithWorkerID(workerID)
	if err != nil {
		panic(err)
	}
	return fn
}
