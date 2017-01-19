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

service TaskManagementService {
	void createTaskSet (1: TaskSetInfo taskSet)

	void updateTaskInfo (1: TaskInfo task)
}

struct StageInfo {
	1: i32 stageId;
	2: string type;	//ShuffleMap or Final stage
	3: i32 jobId;
	4: string appId;
}

struct StageList {
	1: list<StageInfo> stages;
	2: i64 submitTime;
	3: i32 jobId;
	4: string appId;
}

service StageManagementService {
	void createStageList (1: StageList stages)

	void updateStageInfo (1: StageInfo stage)
}

struct JobInfo {
	1: i32 jobId;
	2: string appId;
}

service JobManageMentService {
	void createJob (1: JobInfo job)

	void updateJobInfo(1: JobInfo job)
}