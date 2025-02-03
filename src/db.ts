import { Database } from "bun:sqlite";

export const db = new Database("schedules.db");

// ==================== Database Functions ====================
export function initializeDatabase() {
    db.run(`
    CREATE TABLE IF NOT EXISTS schedules (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      cronExpression TEXT NOT NULL,
      isActive INTEGER DEFAULT 0,
      task TEXT NOT NULL,
      createdAt TEXT NOT NULL,
      updatedAt TEXT NOT NULL,
      lastRun TEXT,
      nextRun TEXT,
      lastStatus TEXT,
      currentJobId TEXT,
      progress INTEGER DEFAULT 0,
      status TEXT DEFAULT 'idle',
      retryAttempts INTEGER DEFAULT 0,
      maxRetries INTEGER DEFAULT 3,
      retryDelay INTEGER DEFAULT 60,
      message TEXT,
			pe TEXT,
			scorecard INTEGER DEFAULT 0,
			ou INTEGER DEFAULT 0,
			includeChildren INTEGER DEFAULT 0
    )
  `);

    db.run(`
    CREATE TABLE IF NOT EXISTS job_executions (
      id TEXT PRIMARY KEY,
      scheduleId TEXT NOT NULL,
      startTime TEXT NOT NULL,
      endTime TEXT,
      status TEXT NOT NULL,
      error TEXT,
      FOREIGN KEY(scheduleId) REFERENCES schedules(id)
    )
  `);
}
