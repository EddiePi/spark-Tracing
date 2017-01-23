namespace java org.apache.spark.tracing

struct TaskSetInfo {
	1: set<TaskInfo> taskSet;
	2: i64 submitTime;
	3: i32 stageId;
	4: i32 stageAttemptId;
	5: i32 jobId;
	6: string appId;
}

struct TaskInfo {
	1: i64 taskId;
	2: i32 stageId;
	3: i32 stageAttempId;
	4: i32 jobId;
	5: string appId;
	6: i64 startTime;
	7: i64 finishTime;
	8: double cpuUsage;
	9: i32 peakMemoryUsage;
	10: string status;	//SUCCESSFUL, FAILED...
}

struct StageInfo {
	1: i32 stageId;
	2: string type;	//ShuffleMap or Final stage
	3: i32 jobId;
	4: string appId;
}

struct JobInfo {
	1: i32 jobId;
	2: string appId;
}

service TracingService {
	void createTaskSet (1: TaskSetInfo taskSet)

	void updateTaskInfo (1: TaskInfo task)

	void createStage (1: StageInfo stage)

	void updateStageInfo (1: StageInfo stage)

	void createJob (1: JobInfo job)

	void updateJobInfo(1: JobInfo job)
}
