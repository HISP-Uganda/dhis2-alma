import { Schedule } from "./interfaces";
import { queryDHIS2 } from "./utils";
import { db } from "./db";
const runManually = async (id: string) => {
    const schedule = db
        .query<Schedule, string>("SELECT * FROM schedules WHERE id = ?")
        .get(id);

    if (!schedule) {
        console.log("fails");
    } else {
        await queryDHIS2(schedule);
    }
};

runManually("7738d782-d9a2-400b-be96-7ea848f34e27").then(() =>
    console.log("Done"),
);
