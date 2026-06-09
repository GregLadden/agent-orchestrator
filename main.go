package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
)

// Task represents a processing job submitted by the user
type Task struct {
	ID        string    `json:"id"`
	Query     string    `json:"query"`
	Workflow  string    `json:"workflow"`
	Scope     string    `json:"scope"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

// Agent represents an available system worker node
type Agent struct {
	ID           string   `json:"id"`
	Name         string   `json:"name"`
	Role         string   `json:"role"`
	Scope        string   `json:"scope"`
	Capabilities []string `json:"capabilities"`
	SystemPrompt string   `json:"system_prompt"`
}

var AgentCatalog = []Agent{
	{
		ID:           "detect",
		Name:         "DetectAgent",
		Role:         "Query Parser and Intent Classifier",
		Scope:        "portfolio",
		Capabilities: []string{"intent-detection", "query-parsing"},
		SystemPrompt: "You are DetectAgent. Your job is to extract user intent, requirements, and target systems from query inputs. Format your output as structured bullet points of intent classification. Keep it short.",
	},
	{
		ID:           "matcher",
		Name:         "TalentMatcherAgent",
		Role:         "Recruiter Candidate Selector",
		Scope:        "portfolio",
		Capabilities: []string{"skill-matching", "candidate-ranking"},
		SystemPrompt: "You are TalentMatcherAgent. You scan and filter candidates by required skills and rank them by suitability. Respond with simulated candidate ranking details. Keep it short.",
	},
	{
		ID:           "monitor",
		Name:         "SystemMonitorAgent",
		Role:         "Infrastructure Telemetry Analyzer",
		Scope:        "portfolio",
		Capabilities: []string{"performance-metrics", "leak-detection"},
		SystemPrompt: "You are SystemMonitorAgent. You analyze telemetry metrics, trace heap memory sizes, and detect leaks. Respond with a system diagnostic log showing metric anomalies. Keep it short.",
	},
}

// LogMessage represents a log entry generated during task execution
type LogMessage struct {
	Timestamp string `json:"timestamp"`
	Agent     string `json:"agent"`
	Message   string `json:"message"`
	Node      string `json:"node"` // current active visual node: detect, catalog, matcher, monitor, orchestrator, idle
}

var (
	rdb         *redis.Client
	ctx         = context.Background()
	queueName   = "agent_tasks_queue"
	logsListKey = "agent_task_logs:%s"
	pubsubKey   = "agent_task_pubsub:%s"
)

func main() {
	loadEnv()
	// 1. Initialize Redis connection
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		redisURL = "redis://localhost:6379"
	}
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		log.Printf("Warning: Failed to parse REDIS_URL (%s). Using default client option.", err)
		opts = &redis.Options{Addr: "localhost:6379"}
	}
	rdb = redis.NewClient(opts)

	// Ping Redis to verify connection
	_, err = rdb.Ping(ctx).Result()
	if err != nil {
		log.Printf("Warning: Redis connection failed (%v). Service will run in mock/in-memory mode.", err)
		rdb = nil
	} else {
		log.Println("Connected to Redis successfully!")
	}

	// 2. Start Worker Pool
	numWorkers := 3
	for i := 1; i <= numWorkers; i++ {
		go startWorker(i)
	}
	log.Printf("Started %d concurrent background workers.", numWorkers)

	// 3. Set up Gin router
	r := gin.Default()

	// CORS Middleware
	r.Use(func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		c.Writer.Header().Set("Access-Control-Allow-Credentials", "true")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization, accept, origin, Cache-Control, X-Requested-With")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS, GET, PUT, DELETE")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}
		c.Next()
	})

	// Routes
	r.GET("/api/health", func(c *gin.Context) {
		status := "healthy"
		redisStatus := "connected"
		if rdb == nil {
			redisStatus = "disconnected (using fallback mock mode)"
		}
		c.JSON(http.StatusOK, gin.H{
			"status": status,
			"redis":  redisStatus,
			"time":   time.Now().Format(time.RFC3339),
		})
	})

	r.POST("/api/tasks", handleCreateTask)
	r.GET("/api/tasks/:id/stream", handleStreamTask)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8081"
	}
	log.Printf("Agent Orchestrator Service starting on port %s...", port)
	if err := r.Run(":" + port); err != nil {
		log.Fatalf("Failed to run HTTP server: %v", err)
	}
}

func handleCreateTask(c *gin.Context) {
	var input struct {
		Query    string `json:"query"`
		Workflow string `json:"workflow"`
		Scope    string `json:"scope"`
	}

	if err := c.ShouldBindJSON(&input); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request body"})
		return
	}

	if input.Query == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Query is required"})
		return
	}

	scope := input.Scope
	if scope == "" {
		scope = "portfolio"
	}

	// Validate that we have agents registered for this scope
	scopeHasAgents := false
	for _, agent := range AgentCatalog {
		if agent.Scope == scope {
			scopeHasAgents = true
			break
		}
	}

	if !scopeHasAgents {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("No agents registered for scope: %q", scope)})
		return
	}

	taskID := fmt.Sprintf("task_%d", time.Now().UnixNano())

	task := Task{
		ID:        taskID,
		Query:     input.Query,
		Workflow:  input.Workflow,
		Scope:     scope,
		Status:    "queued",
		CreatedAt: time.Now(),
	}

	taskBytes, err := json.Marshal(task)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to serialize task"})
		return
	}

	if rdb != nil {
		// Push task onto Redis Queue list
		err = rdb.LPush(ctx, queueName, taskBytes).Err()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to enqueue task"})
			return
		}
	} else {
		// Mock Mode: Run worker simulation in goroutine directly
		go simulateTaskMock(task)
	}

	c.JSON(http.StatusAccepted, gin.H{
		"task_id":    taskID,
		"status":     "queued",
		"scope":      scope,
		"created_at": task.CreatedAt,
	})
}

func handleStreamTask(c *gin.Context) {
	taskID := c.Param("id")
	if taskID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Task ID is required"})
		return
	}

	c.Writer.Header().Set("Content-Type", "text/event-stream")
	c.Writer.Header().Set("Cache-Control", "no-cache")
	c.Writer.Header().Set("Connection", "keep-alive")
	c.Writer.Header().Set("Transfer-Encoding", "chunked")

	// Verify connection is still open
	clientGone := c.Request.Context().Done()

	// If running in Mock Mode, we read from an in-memory session or channel, or Redis fallback
	if rdb == nil {
		// In mock mode, we fallback to a simple poll/listen of a global channels registry.
		// For simplicity, we can listen directly from our simulation using a helper.
		streamMockLogs(c, taskID, clientGone)
		return
	}

	// 1. Flush any existing log history from Redis list to the stream first
	logsKey := fmt.Sprintf(logsListKey, taskID)
	existingLogs, err := rdb.LRange(ctx, logsKey, 0, -1).Result()
	if err == nil {
		for _, logStr := range existingLogs {
			c.SSEvent("message", logStr)
			c.Writer.Flush()
		}
	}

	// 2. Subscribe to Redis pub/sub channel for new logs
	pubsubChannel := fmt.Sprintf(pubsubKey, taskID)
	pubsub := rdb.Subscribe(ctx, pubsubChannel)
	defer pubsub.Close()

	ch := pubsub.Channel()

	for {
		select {
		case <-clientGone:
			log.Printf("Client disconnected from SSE stream for task %s", taskID)
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			c.SSEvent("message", msg.Payload)
			c.Writer.Flush()

			// Check if this log message signifies completion
			var logMsg LogMessage
			if err := json.Unmarshal([]byte(msg.Payload), &logMsg); err == nil {
				if logMsg.Agent == "Orchestrator" && (logMsg.Message == "Workflow execution completed successfully. Streaming disconnected." || logMsg.Message == "Workflow execution completed. Alert dispatched to DevOps.") {
					// End stream
					return
				}
			}
		}
	}
}

func startWorker(workerID int) {
	if rdb == nil {
		return
	}

	log.Printf("[Worker %d] Started task poller loop...", workerID)
	for {
		// Block pop from the Redis queue
		result, err := rdb.BRPop(ctx, 0, queueName).Result()
		if err != nil {
			log.Printf("[Worker %d] Error popping from queue: %v. Retrying in 1s...", workerID, err)
			time.Sleep(1 * time.Second)
			continue
		}

		// result[0] is the queue name, result[1] is the element value
		taskBytes := result[1]
		var task Task
		if err := json.Unmarshal([]byte(taskBytes), &task); err != nil {
			log.Printf("[Worker %d] Failed to parse task: %v", workerID, err)
			continue
		}

		log.Printf("[Worker %d] Processing task %s (Workflow: %s)", workerID, task.ID, task.Workflow)
		executeTask(task, workerID)
	}
}

type CacheLookupRequest struct {
	Text      string  `json:"text"`
	Threshold float64 `json:"threshold"`
}

type CacheLookupResponse struct {
	Hit    bool    `json:"hit"`
	Answer string  `json:"answer"`
	Score  float64 `json:"score"`
}

type CacheUpsertRequest struct {
	Query  string `json:"query"`
	Answer string `json:"answer"`
}

func querySemanticCache(queryText string) (string, float64, bool) {
	ragURL := os.Getenv("RAG_CACHE_SERVICE_URL")
	if ragURL == "" {
		ragURL = "http://localhost:8000"
	}

	reqPayload := CacheLookupRequest{
		Text:      queryText,
		Threshold: 0.85,
	}

	reqBytes, err := json.Marshal(reqPayload)
	if err != nil {
		return "", 0, false
	}

	resp, err := http.Post(fmt.Sprintf("%s/cache/lookup", ragURL), "application/json", bytes.NewBuffer(reqBytes))
	if err != nil {
		return "", 0, false
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", 0, false
	}

	var lookupResp CacheLookupResponse
	if err := json.NewDecoder(resp.Body).Decode(&lookupResp); err != nil {
		return "", 0, false
	}

	return lookupResp.Answer, lookupResp.Score, lookupResp.Hit
}

func saveToSemanticCache(queryText, answerText string) {
	ragURL := os.Getenv("RAG_CACHE_SERVICE_URL")
	if ragURL == "" {
		ragURL = "http://localhost:8000"
	}

	reqPayload := CacheUpsertRequest{
		Query:  queryText,
		Answer: answerText,
	}

	reqBytes, err := json.Marshal(reqPayload)
	if err != nil {
		return
	}

	resp, err := http.Post(fmt.Sprintf("%s/cache/upsert", ragURL), "application/json", bytes.NewBuffer(reqBytes))
	if err == nil {
		resp.Body.Close()
	}
}

func runCacheHitWorkflow(task Task, answer string, score float64, pushLog func(LogMessage)) {
	targetAgentName := "TalentMatcherAgent"
	if task.Workflow == "telemetry" {
		targetAgentName = "SystemMonitorAgent"
	}
	
	pushLog(LogMessage{
		Timestamp: time.Now().Format("3:04:05 PM"),
		Agent:     "Semantic Cache",
		Message:   fmt.Sprintf("Checking semantic cache database for query..."),
		Node:      "cache",
	})
	time.Sleep(400 * time.Millisecond)

	pushLog(LogMessage{
		Timestamp: time.Now().Format("3:04:05 PM"),
		Agent:     "Semantic Cache",
		Message:   fmt.Sprintf("Semantic cache HIT (similarity score: %.1f%%). Bypassing LLM execution pool.", score*100),
		Node:      "orchestrator",
	})
	time.Sleep(400 * time.Millisecond)

	pushLog(LogMessage{
		Timestamp: time.Now().Format("3:04:05 PM"),
		Agent:     targetAgentName,
		Message:   answer,
		Node:      task.Workflow,
	})
	time.Sleep(400 * time.Millisecond)

	msg := "Workflow execution completed successfully. Streaming disconnected."
	if task.Workflow == "telemetry" {
		msg = "Workflow execution completed. Alert dispatched to DevOps."
	}
	pushLog(LogMessage{
		Timestamp: time.Now().Format("3:04:05 PM"),
		Agent:     "Orchestrator",
		Message:   msg,
		Node:      "orchestrator",
	})
}

func executeTask(task Task, workerID int) {
	logsKey := fmt.Sprintf(logsListKey, task.ID)
	pubsubChannel := fmt.Sprintf(pubsubKey, task.ID)

	pushLog := func(logMsg LogMessage) {
		logBytes, _ := json.Marshal(logMsg)
		logStr := string(logBytes)

		// 1. Push to task logs list in Redis (for history)
		rdb.RPush(ctx, logsKey, logStr)
		rdb.Expire(ctx, logsKey, 1*time.Hour)

		// 2. Publish to Redis Pub/Sub channel (for real-time streaming)
		rdb.Publish(ctx, pubsubChannel, logStr)
	}

	// Try semantic cache lookup first
	if answer, score, hit := querySemanticCache(task.Query); hit {
		log.Printf("[Worker %d] Semantic cache HIT for task %s", workerID, task.ID)
		runCacheHitWorkflow(task, answer, score, pushLog)
		return
	}

	// Try real LLM execution first
	if os.Getenv("GEMINI_API_KEY") != "" {
		err := executeTaskWithLLM(task, pushLog)
		if err == nil {
			log.Printf("[Worker %d] Finished task %s via Gemini LLM", workerID, task.ID)
			return
		}
		log.Printf("[Worker %d] LLM execution failed (%v). Falling back to mock workflow.", workerID, err)
	}

	// Fallback to Mock Steps
	steps := getWorkflowSteps(task)
	for _, step := range steps {
		logMsg := LogMessage{
			Timestamp: time.Now().Format("3:04:05 PM"),
			Agent:     step.Agent,
			Message:   step.Message,
			Node:      step.Node,
		}
		pushLog(logMsg)
		time.Sleep(time.Duration(step.DelayMs) * time.Millisecond)
	}

	// Save final mock answer to semantic cache for future hits
	var finalMsg string
	if task.Workflow == "telemetry" {
		finalMsg = "**SystemMonitorAgent: Telemetry Diagnostic Report**\n\n*   **Status**: Heap Growth Leak Detected (+140MB/hr)\n*   **Target**: /recommend microservice endpoints\n*   **Origin**: sync.Pool allocation in recommendation_handler.go:L78\n\n*DevOps alert dispatched successfully.*"
	} else {
		finalMsg = "**TalentMatcherAgent: Candidate Audit Report**\n\n*   **Alice** (95% fit) - Dell Technologies, Austin. Golang expert, AutoGen user.\n*   **Bob** (88% fit) - Experienced Golang developer.\n*   **Charlie** (72% fit) - Junior candidate.\n\n*Resume audit report generated and reach-out email drafted successfully.*"
	}
	go saveToSemanticCache(task.Query, finalMsg)

	log.Printf("[Worker %d] Finished task %s (Mock Mode)", workerID, task.ID)
}

// Memory logs cache for Mock Mode
var (
	mockLogsMutex sync.RWMutex
	mockLogsMap   = make(map[string][]LogMessage)
	mockStatusMap = make(map[string]bool)
)

func simulateTaskMock(task Task) {
	mockLogsMutex.Lock()
	mockStatusMap[task.ID] = false
	mockLogsMutex.Unlock()

	pushLog := func(logMsg LogMessage) {
		mockLogsMutex.Lock()
		logs := mockLogsMap[task.ID]
		logs = append(logs, logMsg)
		mockLogsMap[task.ID] = logs
		mockLogsMutex.Unlock()
	}

	// Try semantic cache lookup first
	if answer, score, hit := querySemanticCache(task.Query); hit {
		log.Printf("[Mock Simulation] Semantic cache HIT for task %s", task.ID)
		runCacheHitWorkflow(task, answer, score, pushLog)
		mockLogsMutex.Lock()
		mockStatusMap[task.ID] = true
		mockLogsMutex.Unlock()
		return
	}

	// Try real LLM execution first
	if os.Getenv("GEMINI_API_KEY") != "" {
		err := executeTaskWithLLM(task, pushLog)
		if err == nil {
			mockLogsMutex.Lock()
			mockStatusMap[task.ID] = true
			mockLogsMutex.Unlock()
			log.Printf("[Mock Simulation] Finished task %s via Gemini LLM", task.ID)
			return
		}
		log.Printf("[Mock Simulation] LLM execution failed (%v). Falling back to mock workflow.", err)
	}

	// Fallback mock steps
	steps := getWorkflowSteps(task)
	for _, step := range steps {
		logMsg := LogMessage{
			Timestamp: time.Now().Format("3:04:05 PM"),
			Agent:     step.Agent,
			Message:   step.Message,
			Node:      step.Node,
		}
		pushLog(logMsg)
		time.Sleep(time.Duration(step.DelayMs) * time.Millisecond)
	}

	// Save final mock answer to semantic cache for future hits
	var finalMsg string
	if task.Workflow == "telemetry" {
		finalMsg = "**SystemMonitorAgent: Telemetry Diagnostic Report**\n\n*   **Status**: Heap Growth Leak Detected (+140MB/hr)\n*   **Target**: /recommend microservice endpoints\n*   **Origin**: sync.Pool allocation in recommendation_handler.go:L78\n\n*DevOps alert dispatched successfully.*"
	} else {
		finalMsg = "**TalentMatcherAgent: Candidate Audit Report**\n\n*   **Alice** (95% fit) - Dell Technologies, Austin. Golang expert, AutoGen user.\n*   **Bob** (88% fit) - Experienced Golang developer.\n*   **Charlie** (72% fit) - Junior candidate.\n\n*Resume audit report generated and reach-out email drafted successfully.*"
	}
	go saveToSemanticCache(task.Query, finalMsg)

	mockLogsMutex.Lock()
	mockStatusMap[task.ID] = true
	mockLogsMutex.Unlock()
}

func executeTaskWithLLM(task Task, pushLog func(LogMessage)) error {
	// 1. Detect Agent step
	pushLog(LogMessage{
		Timestamp: time.Now().Format("3:04:05 PM"),
		Agent:     "DetectAgent",
		Message:   fmt.Sprintf("Analyzing query: %q (Scope: %s)", task.Query, task.Scope),
		Node:      "detect",
	})
	time.Sleep(500 * time.Millisecond)

	// Filter agents in catalog matching task scope
	var scopedAgents []Agent
	for _, agent := range AgentCatalog {
		if agent.Scope == task.Scope {
			scopedAgents = append(scopedAgents, agent)
		}
	}

	if len(scopedAgents) == 0 {
		return fmt.Errorf("no agents found in scope: %s", task.Scope)
	}

	// Find DetectAgent
	var detectAgent Agent
	foundDetect := false
	for _, a := range scopedAgents {
		if a.ID == "detect" {
			detectAgent = a
			foundDetect = true
			break
		}
	}
	if !foundDetect {
		detectAgent = scopedAgents[0]
	}

	detectResult, err := callGeminiAPI(detectAgent.SystemPrompt, task.Query)
	if err != nil {
		return fmt.Errorf("DetectAgent inference failed: %w", err)
	}

	pushLog(LogMessage{
		Timestamp: time.Now().Format("3:04:05 PM"),
		Agent:     "DetectAgent",
		Message:   detectResult,
		Node:      "detect",
	})
	time.Sleep(1 * time.Second)

	// 2. Routing step (Agent Catalog)
	targetAgentID := "matcher" // default
	if task.Workflow == "telemetry" || bytes.Contains([]byte(detectResult), []byte("SYSTEM_DIAGNOSTIC")) || bytes.Contains([]byte(detectResult), []byte("telemetry")) || bytes.Contains([]byte(detectResult), []byte("leak")) {
		targetAgentID = "monitor"
	}

	var targetAgent Agent
	foundTarget := false
	for _, a := range scopedAgents {
		if a.ID == targetAgentID {
			targetAgent = a
			foundTarget = true
			break
		}
	}
	if !foundTarget {
		for _, a := range scopedAgents {
			if a.ID != "detect" {
				targetAgent = a
				foundTarget = true
				break
			}
		}
		if !foundTarget {
			targetAgent = scopedAgents[0]
		}
	}

	pushLog(LogMessage{
		Timestamp: time.Now().Format("3:04:05 PM"),
		Agent:     "AgentCatalog",
		Message:   fmt.Sprintf("Matching target agent: %q (Role: %s, Scope: %s, Score: 0.98/1.0)", targetAgent.Name, targetAgent.Role, targetAgent.Scope),
		Node:      "catalog",
	})
	time.Sleep(1 * time.Second)

	// 3. Target Agent step
	nodeName := "matcher"
	if targetAgent.ID == "monitor" {
		nodeName = "monitor"
	}

	pushLog(LogMessage{
		Timestamp: time.Now().Format("3:04:05 PM"),
		Agent:     targetAgent.Name,
		Message:   fmt.Sprintf("Executing agent task request with instruction context..."),
		Node:      nodeName,
	})
	time.Sleep(500 * time.Millisecond)

	targetInput := fmt.Sprintf("Query: %s\n\nIntent Extraction context:\n%s", task.Query, detectResult)
	targetResult, err := callGeminiAPI(targetAgent.SystemPrompt, targetInput)
	if err != nil {
		return fmt.Errorf("%s inference failed: %w", targetAgent.Name, err)
	}

	pushLog(LogMessage{
		Timestamp: time.Now().Format("3:04:05 PM"),
		Agent:     targetAgent.Name,
		Message:   targetResult,
		Node:      nodeName,
	})
	go saveToSemanticCache(task.Query, targetResult)
	time.Sleep(1 * time.Second)

	// 4. Orchestrator finish
	msg := "Workflow execution completed successfully. Streaming disconnected."
	if targetAgent.ID == "monitor" {
		msg = "Workflow execution completed. Alert dispatched to DevOps."
	}
	pushLog(LogMessage{
		Timestamp: time.Now().Format("3:04:05 PM"),
		Agent:     "Orchestrator",
		Message:   msg,
		Node:      "orchestrator",
	})

	return nil
}

func streamMockLogs(c *gin.Context, taskID string, clientGone <-chan struct{}) {
	lastIdx := 0
	for {
		select {
		case <-clientGone:
			return
		default:
			mockLogsMutex.RLock()
			logs, exists := mockLogsMap[taskID]
			var logsCopy []LogMessage
			if exists {
				logsCopy = make([]LogMessage, len(logs))
				copy(logsCopy, logs)
			}
			done, doneOk := mockStatusMap[taskID]
			mockLogsMutex.RUnlock()

			if exists && len(logsCopy) > lastIdx {
				for i := lastIdx; i < len(logsCopy); i++ {
					logBytes, _ := json.Marshal(logsCopy[i])
					c.SSEvent("message", string(logBytes))
					c.Writer.Flush()
				}
				lastIdx = len(logsCopy)
			}

			if doneOk && done && lastIdx >= len(logsCopy) {
				return
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
}

type StepTemplate struct {
	Agent   string
	Message string
	Node    string
	DelayMs int
}

func getWorkflowSteps(task Task) []StepTemplate {
	wf := task.Workflow
	if wf == "" {
		if rand.Float32() > 0.5 {
			wf = "recruit"
		} else {
			wf = "telemetry"
		}
	}

	if wf == "recruit" {
		return []StepTemplate{
			{Agent: "DetectAgent", Message: fmt.Sprintf("Analyzing query: %q", task.Query), Node: "detect", DelayMs: 800},
			{Agent: "DetectAgent", Message: "Extracted Intent: SKILL_MATCHING, TARGET: Candidates, REQUIREMENTS: Golang, AutoGen", Node: "detect", DelayMs: 600},
			{Agent: "DetectAgent", Message: "Querying Agent Catalog for capabilities: [golang, agent-orchestration]", Node: "detect", DelayMs: 800},
			{Agent: "AgentCatalog", Message: "Matching target agent: \"TalentMatcherAgent\" (Score: 0.98/1.0)", Node: "catalog", DelayMs: 1000},
			{Agent: "Orchestrator", Message: "Establishing persistent streaming connection via Server-Sent Events...", Node: "orchestrator", DelayMs: 800},
			{Agent: "TalentMatcherAgent", Message: "Scanning candidate pool database...", Node: "matcher", DelayMs: 1200},
			{Agent: "TalentMatcherAgent", Message: "Found 3 candidates: Alice (95%), Bob (88%), Charlie (72%)", Node: "matcher", DelayMs: 1000},
			{Agent: "TalentMatcherAgent", Message: "Candidate 1: Alice (Dell Technologies, Austin) - Golang expert, AutoGen user.", Node: "matcher", DelayMs: 800},
			{Agent: "TalentMatcherAgent", Message: "Generating resume audit report and drafting reach-out email template...", Node: "matcher", DelayMs: 1200},
			{Agent: "Orchestrator", Message: "Workflow execution completed successfully. Streaming disconnected.", Node: "orchestrator", DelayMs: 500},
		}
	} else if wf == "telemetry" {
		return []StepTemplate{
			{Agent: "DetectAgent", Message: fmt.Sprintf("Analyzing query: %q", task.Query), Node: "detect", DelayMs: 800},
			{Agent: "DetectAgent", Message: "Extracted Intent: SYSTEM_DIAGNOSTIC, TARGET: RecommendationService", Node: "detect", DelayMs: 600},
			{Agent: "DetectAgent", Message: "Querying Agent Catalog for capabilities: [telemetry, performance-metrics]", Node: "detect", DelayMs: 800},
			{Agent: "AgentCatalog", Message: "Matching target agent: \"SystemMonitorAgent\" (Score: 0.95/1.0)", Node: "catalog", DelayMs: 1000},
			{Agent: "Orchestrator", Message: "Invoking SystemMonitorAgent via secure API gateway...", Node: "orchestrator", DelayMs: 800},
			{Agent: "SystemMonitorAgent", Message: "Querying Prometheus server for container memory utilization (last 24h)...", Node: "monitor", DelayMs: 1200},
			{Agent: "SystemMonitorAgent", Message: "Detected Heap Growth pattern (+140MB/hr) in /recommend endpoints. Potential Leak.", Node: "monitor", DelayMs: 1200},
			{Agent: "SystemMonitorAgent", Message: "Fetching Go pprof profiling trace data...", Node: "monitor", DelayMs: 1000},
			{Agent: "SystemMonitorAgent", Message: "Pinpointed leak origin: sync.Pool allocation in recommendation_handler.go:L78.", Node: "monitor", DelayMs: 1200},
			{Agent: "Orchestrator", Message: "Workflow execution completed. Alert dispatched to DevOps.", Node: "orchestrator", DelayMs: 500},
		}
	}

	return []StepTemplate{
		{Agent: "DetectAgent", Message: fmt.Sprintf("Parsing custom query: %q", task.Query), Node: "detect", DelayMs: 800},
		{Agent: "DetectAgent", Message: "Intent detected: GENERAL_ORCHESTRATION, matching capabilities dynamically...", Node: "detect", DelayMs: 600},
		{Agent: "AgentCatalog", Message: "Matched specialized worker node: TalentMatcherAgent", Node: "catalog", DelayMs: 1000},
		{Agent: "Orchestrator", Message: "Routing task thread to TalentMatcherAgent worker group...", Node: "orchestrator", DelayMs: 800},
		{Agent: "TalentMatcherAgent", Message: "Executing request context simulation...", Node: "matcher", DelayMs: 1500},
		{Agent: "TalentMatcherAgent", Message: fmt.Sprintf("Success: Executed custom workflow for %q", task.Query), Node: "matcher", DelayMs: 1200},
		{Agent: "Orchestrator", Message: "Workflow execution completed successfully. Streaming disconnected.", Node: "orchestrator", DelayMs: 500},
	}
}

// Gemini API structures & client
type GeminiRequest struct {
	SystemInstruction *GeminiSystemInstruction `json:"systemInstruction,omitempty"`
	Contents          []GeminiContent          `json:"contents"`
}

type GeminiSystemInstruction struct {
	Parts []GeminiPart `json:"parts"`
}

type GeminiContent struct {
	Role  string       `json:"role"`
	Parts []GeminiPart `json:"parts"`
}

type GeminiPart struct {
	Text string `json:"text"`
}

type GeminiResponse struct {
	Candidates []struct {
		Content struct {
			Parts []struct {
				Text string `json:"text"`
			} `json:"parts"`
		} `json:"content"`
	} `json:"candidates"`
}

func loadEnv() {
	file, err := os.Open(".env")
	if err != nil {
		return
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return
	}

	lines := bytes.Split(data, []byte("\n"))
	for _, line := range lines {
		line = bytes.TrimSpace(line)
		if len(line) == 0 || bytes.HasPrefix(line, []byte("#")) {
			continue
		}
		parts := bytes.SplitN(line, []byte("="), 2)
		if len(parts) == 2 {
			key := string(bytes.TrimSpace(parts[0]))
			val := string(bytes.TrimSpace(parts[1]))
			if len(val) >= 2 && val[0] == '"' && val[len(val)-1] == '"' {
				val = val[1 : len(val)-1]
			} else if len(val) >= 2 && val[0] == '\'' && val[len(val)-1] == '\'' {
				val = val[1 : len(val)-1]
			}
			os.Setenv(key, val)
		}
	}
}

func callGeminiAPI(systemPrompt, userQuery string) (string, error) {
	apiKey := os.Getenv("GEMINI_API_KEY")
	if apiKey == "" {
		return "", fmt.Errorf("GEMINI_API_KEY environment variable is not set")
	}

	apiURL := fmt.Sprintf("https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key=%s", apiKey)

	reqPayload := GeminiRequest{
		SystemInstruction: &GeminiSystemInstruction{
			Parts: []GeminiPart{{Text: systemPrompt}},
		},
		Contents: []GeminiContent{
			{
				Role:  "user",
				Parts: []GeminiPart{{Text: userQuery}},
			},
		},
	}

	reqBytes, err := json.Marshal(reqPayload)
	if err != nil {
		return "", fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequest("POST", apiURL, bytes.NewBuffer(reqBytes))
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to send API request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var geminiResp GeminiResponse
	if err := json.NewDecoder(resp.Body).Decode(&geminiResp); err != nil {
		return "", fmt.Errorf("failed to decode response: %w", err)
	}

	if len(geminiResp.Candidates) == 0 || len(geminiResp.Candidates[0].Content.Parts) == 0 {
		return "", fmt.Errorf("empty response received from API")
	}

	return geminiResp.Candidates[0].Content.Parts[0].Text, nil
}
