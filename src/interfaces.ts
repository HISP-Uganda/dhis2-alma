export interface RetryConfig {
    maxAttempts: number;
    delaySeconds: number;
}

export interface Schedule {
    id: string;
    name: string;
    cronExpression: string;
    isActive: boolean;
    task: string;
    createdAt: Date;
    updatedAt: Date;
    lastRun?: Date;
    nextRun?: Date | undefined | null;
    progress?: number;
    status?: "idle" | "running" | "completed" | "failed";
    lastStatus?: string;
    currentJobId?: string;
    retryAttempts?: number;
    maxRetries: number;
    retryDelay: number;
    pe: string;
    scorecard: number;
    ou: string;
    includeChildren: boolean;
}

export interface ProgressUpdate {
    scheduleId: string;
    progress: number;
    status: "idle" | "running" | "completed" | "failed";
    message?: string;
    timestamp: Date;
}
