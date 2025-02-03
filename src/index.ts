import { Hono } from "hono";
import { cors } from "hono/cors";
import { logger } from "hono/logger";
import { prettyJSON } from "hono/pretty-json";
import { stream } from "hono/streaming";
import * as cron from "node-cron";
import { db, initializeDatabase } from "./db";
import { ProgressUpdate, Schedule } from "./interfaces";
import {
    isValidCronExpression,
    restoreActiveSchedules,
    scheduleFromRow,
    startScheduleJob,
} from "./schedule";
import { queryDHIS2 } from "./utils";

// ==================== Global State ====================
export const progressSubscribers: Map<
    string,
    Set<(data: string) => void>
> = new Map();

const runningJobs: Map<string, cron.ScheduledTask> = new Map();

export const app = new Hono();

function initializeSystem(func: (schedule: Schedule) => Promise<void>) {
    console.log("Initializing schedule system...");
    initializeDatabase();
    // cleanupRunningJobs();
    restoreActiveSchedules(runningJobs, func);
    console.log("Schedule system initialized");
}

// ==================== Middleware ====================
app.use("*", logger());
app.use("*", prettyJSON());
app.use("*", cors());

// ==================== Routes ====================
// Create a new schedule
app.post("/schedules", async (c) => {
    try {
        const body = await c.req.json();
        const {
            name,
            cronExpression,
            task,
            maxRetries = 3,
            retryDelay = 60,
            pe,
            ou,
            scorecard,
            includeChildren,
        } = body;

        if (!name || !cronExpression || !task) {
            return c.json({ error: "Missing required fields" }, 400);
        }

        if (!isValidCronExpression(cronExpression)) {
            return c.json({ error: "Invalid cron expression" }, 400);
        }

        // Validate retry configuration
        if (maxRetries < 0 || maxRetries > 10) {
            return c.json(
                { error: "maxRetries must be between 0 and 10" },
                400,
            );
        }

        if (retryDelay < 0 || retryDelay > 3600) {
            return c.json(
                { error: "retryDelay must be between 0 and 3600 seconds" },
                400,
            );
        }

        const id = crypto.randomUUID();
        const now = new Date().toISOString();

        db.run(
            `INSERT INTO schedules (id, name, cronExpression, task, createdAt, updatedAt, isActive, progress, status, maxRetries, retryDelay,pe,ou,scorecard,includeChildren) VALUES (?, ?, ?, ?, ?, ?, 0, 0, 'idle', ?, ?, ?, ?, ?, ?)`,
            [
                id,
                name,
                cronExpression,
                task,
                now,
                now,
                maxRetries,
                retryDelay,
                pe,
                ou,
                scorecard,
                includeChildren,
            ],
        );

        const schedule = db
            .query<Schedule, string>("SELECT * FROM schedules WHERE id = ?")
            .get(id);

        if (!schedule) {
            return c.json({ error: "Schedule not found" }, 404);
        }
        return c.json(
            {
                message: "Schedule created successfully",
                schedule: scheduleFromRow(schedule),
            },
            201,
        );
    } catch (error) {
        console.log(error);
        return c.json({ error: "Invalid request body" }, 400);
    }
});

// Get all schedules
app.get("/schedules", (c) => {
    const schedules = db
        .query<Schedule, null>("SELECT * FROM schedules")
        .all(null);
    return c.json({
        schedules: schedules.map(scheduleFromRow),
    });
});

// Get a specific schedule
app.get("/schedules/:id", (c) => {
    const id = c.req.param("id");
    const schedule = db
        .query<Schedule, string>("SELECT * FROM schedules WHERE id = ?")
        .get(id);

    if (!schedule) {
        return c.json({ error: "Schedule not found" }, 404);
    }

    return c.json({ schedule: scheduleFromRow(schedule) });
});

// Edit a specific schedule
app.put("/schedules/:id", async (c) => {
    const id = c.req.param("id");
    const currentSchedule = await c.req.json<Partial<Schedule>>();
    const schedule = db
        .query<Schedule, string>("SELECT * FROM schedules WHERE id = ?")
        .get(id);

    if (!schedule) {
        return c.json({ error: "Schedule not found" }, 404);
    }
    const merged = { ...schedule, ...currentSchedule };

    const now = new Date().toISOString();

    console.log(now);

    db.run(
        `UPDATE schedules set name = ?, cronExpression = ?, task = ?, updatedAt = ?, maxRetries = ?, retryDelay = ?, pe = ?, ou = ?, scorecard = ?, includeChildren = ? WHERE id = ?`,
        [
            merged.name,
            merged.cronExpression,
            merged.task,
            now,
            merged.maxRetries,
            merged.retryDelay,
            merged.pe,
            merged.ou,
            merged.scorecard,
            merged.includeChildren,
            id,
        ],
    );

    return c.json({
        schedule: scheduleFromRow(merged),
    });
});

// Start a schedule
app.post("/schedules/:id/start", async (c) => {
    const id = c.req.param("id");
    const scheduleRow = db
        .query<Schedule, string>("SELECT * FROM schedules WHERE id = ?")
        .get(id);

    if (!scheduleRow) {
        return c.json({ error: "Schedule not found" }, 404);
    }

    const schedule = scheduleFromRow(scheduleRow);

    if (schedule.isActive) {
        return c.json({ error: "Schedule is already running" }, 400);
    }

    try {
        startScheduleJob(schedule, runningJobs, queryDHIS2);
        db.run(
            `UPDATE schedules SET isActive = 1, updatedAt = ?, status = 'idle', progress = 0 WHERE id = ?`,
            [new Date().toISOString(), id],
        );

        const updatedSchedule = db
            .query<Schedule, string>("SELECT * FROM schedules WHERE id = ?")
            .get(id);

        if (!updatedSchedule) {
            return c.json({ error: "Schedule not found" }, 404);
        }
        return c.json({
            message: "Schedule started successfully",
            schedule: scheduleFromRow(updatedSchedule),
        });
    } catch (error: any) {
        console.log(error.message);
        return c.json({ error: "Failed to start schedule" }, 500);
    }
});

// Stop a schedule
app.post("/schedules/:id/stop", async (c) => {
    const id = c.req.param("id");
    const scheduleRow = db
        .query<Schedule, string>("SELECT * FROM schedules WHERE id = ?")
        .get(id);

    if (!scheduleRow) {
        return c.json({ error: "Schedule not found" }, 404);
    }

    const schedule = scheduleFromRow(scheduleRow);

    if (!schedule.isActive) {
        return c.json({ error: "Schedule is not running" }, 400);
    }

    try {
        const job = runningJobs.get(id);
        if (job) {
            job.stop();
            runningJobs.delete(id);
        }

        db.run(
            `UPDATE schedules SET isActive = 0, updatedAt = ?, status = 'idle' WHERE id = ?`,
            [new Date().toISOString(), id],
        );

        const updatedSchedule = db
            .query<Schedule, string>("SELECT * FROM schedules WHERE id = ?")
            .get(id);

        if (!updatedSchedule) {
            return c.json({ error: "Schedule not found" }, 404);
        }
        return c.json({
            message: "Schedule stopped successfully",
            schedule: scheduleFromRow(updatedSchedule),
        });
    } catch (error) {
        return c.json({ error: "Failed to stop schedule" }, 500);
    }
});

// Delete a schedule
app.delete("/schedules/:id", async (c) => {
    const id = c.req.param("id");
    const schedule = db
        .query<Schedule, string>("SELECT * FROM schedules WHERE id = ?")
        .get(id);

    if (!schedule) {
        return c.json({ error: "Schedule not found" }, 404);
    }

    try {
        if (schedule.isActive) {
            const job = runningJobs.get(id);
            if (job) {
                job.stop();
                runningJobs.delete(id);
            }
        }

        db.run("DELETE FROM schedules WHERE id = ?", [id]);
        return c.json({ message: "Schedule deleted successfully" });
    } catch (error) {
        return c.json({ error: "Failed to delete schedule" }, 500);
    }
});

// Subscribe to schedule progress
app.get("/schedules/:id/progress", async (c) => {
    const id = c.req.param("id");
    const schedule = db
        .query<Schedule, string>("SELECT * FROM schedules WHERE id = ?")
        .get(id);

    if (!schedule) {
        return c.json({ error: "Schedule not found" }, 404);
    }

    c.header("Content-Type", "text/event-stream");
    c.header("Cache-Control", "no-cache");
    c.header("Connection", "keep-alive");

    return stream(c, async (stream) => {
        const subscriber = (data: string) => {
            stream.write(data);
        };

        if (!progressSubscribers.has(id)) {
            progressSubscribers.set(id, new Set());
        }
        progressSubscribers.get(id)?.add(subscriber);

        const initialUpdate: ProgressUpdate = {
            scheduleId: id,
            progress: schedule.progress || 0,
            status: schedule.status || "idle",
            timestamp: new Date(),
        };
        subscriber(`data: ${JSON.stringify(initialUpdate)}\n\n`);

        stream.onAbort(() => {
            progressSubscribers.get(id)?.delete(subscriber);
            if (progressSubscribers.get(id)?.size === 0) {
                progressSubscribers.delete(id);
            }
        });
    });
});

// ==================== Initialize and Export ====================
initializeSystem(async (schedule) => {
    await queryDHIS2(schedule);
});

export default {
    port: 3003,
    fetch: app.fetch,
};
