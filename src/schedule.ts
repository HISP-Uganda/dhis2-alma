import * as cronParser from "cron-parser";
import * as cron from "node-cron";
import { progressSubscribers } from ".";
import { db } from "./db";
import { ProgressUpdate, Schedule } from "./interfaces";

export function cleanupRunningJobs() {
    const runningJobs = db
        .query<Schedule, string>("SELECT * FROM schedules WHERE status = ?")
        .all("running");

    for (const job of runningJobs) {
        db.run(
            `UPDATE schedules SET status = 'failed', progress = ?, updatedAt = ?,lastRun = ? WHERE id = ?`,
            [0, new Date().toISOString(), new Date().toISOString(), job.id],
        );

        broadcastProgress({
            scheduleId: job.id,
            progress: 0,
            status: "failed",
            message: "Task interrupted due to server restart",
            timestamp: new Date(),
        });
    }
}

export function isValidCronExpression(cronExp: string): boolean {
    return cron.validate(cronExp);
}

export function broadcastProgress(update: ProgressUpdate) {
    const subscribers = progressSubscribers.get(update.scheduleId);
    if (subscribers) {
        const eventData = `data: ${JSON.stringify(update)}\n\n`;
        subscribers.forEach((subscriber) => subscriber(eventData));
    }
}

export function getNextRunTime(cronExp: string): Date | null {
    try {
        const interval = cronParser.parseExpression(cronExp);
        return interval.next().toDate();
    } catch (error) {
        return null;
    }
}

export function scheduleFromRow(row: Schedule): Schedule {
    return {
        id: row.id,
        name: row.name,
        cronExpression: row.cronExpression,
        isActive: Boolean(row.isActive),
        task: row.task,
        createdAt: new Date(row.createdAt),
        updatedAt: new Date(row.updatedAt),
        lastRun: row.lastRun ? new Date(row.lastRun) : undefined,
        progress: row.progress,
        status: row.status as Schedule["status"],
        lastStatus: row.lastStatus,
        currentJobId: row.currentJobId,
        nextRun: row.isActive ? getNextRunTime(row.cronExpression) : undefined,
        maxRetries: row.maxRetries,
        retryDelay: row.retryDelay,
        retryAttempts: row.retryAttempts,
        includeChildren: row.includeChildren,
        pe: row.pe,
        ou: row.ou,
        scorecard: row.scorecard,
    };
}
// ==================== Utility Functions ====================

export function startScheduleJob(
    schedule: Schedule,
    runningJobs: Map<string, cron.ScheduledTask>,
    func: (schedule: Schedule) => Promise<void>,
) {
    const job = cron.schedule(
        schedule.cronExpression,
        async () => {
            console.log(
                `Executing task for schedule ${schedule.id}: ${schedule.task}`,
            );

            try {
                await func(schedule);
            } catch (error) {
                console.error(
                    `Error executing task for schedule ${schedule.id}:`,
                    error,
                );
            }
        },
        { timezone: "Africa/Nairobi" },
    );
    job.start();
    runningJobs.set(schedule.id, job);
    return job;
}

// ==================== Initialization Functions ====================

export function restoreActiveSchedules(
    runningJobs: Map<string, cron.ScheduledTask>,
    func: (schedule: Schedule) => Promise<void>,
) {
    console.log("Restoring active schedules...");
    const activeSchedules = db
        .query<Schedule, null>("SELECT * FROM schedules WHERE isActive = 1")
        .all(null);
    for (const scheduleRow of activeSchedules) {
        const schedule = scheduleFromRow(scheduleRow);
        console.log(`Restoring schedule: ${schedule.name} (${schedule.id})`);
        startScheduleJob(schedule, runningJobs, func);
    }
}
