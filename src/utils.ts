import axios from "axios";
import { Schedule } from "./interfaces";
import { db } from "./db";

const NOT_FOR_PROFIT = ["svd8pMum32y", "LrvtF9Umvsh"];
const NOT_FOR_PROFIT_INDICATIONS = [
    "ofZGItap633",
    "LbXgcyeBgZy",
    "HF37g2iSiZB",
    "VACcvy5d4vu",
];

export const almaApi = axios.create({
    baseURL: String(process.env.BASE_URL),
});

export const dhis2Api = axios.create({
    baseURL: process.env.DHIS2_URL,
    auth: {
        username: String(process.env.DHIS2_USERNAME),
        password: String(process.env.DHIS2_PASSWORD),
    },
});

export const sendToAlma = async ({
    data,
    scorecard,
    name,
}: {
    data: unknown;
    scorecard: number;
    name: string;
}): Promise<void> => {
    const response = await almaApi.post("session", {
        backend: String(process.env.BACKEND),
        username: String(process.env.USERNAME),
        password: String(process.env.PASSWORD),
    });
    const headers = response.headers["set-cookie"];
    if (headers) {
        try {
            const form = new FormData();
            const jsonBlob = new Blob(
                [JSON.stringify({ dataValues: [data] })],
                {
                    type: "application/json",
                },
            );
            form.append("file", jsonBlob, "temp.json");
            console.log(`Uploading data for ${name} to ALMA`);
            await almaApi.put(`scorecard/${scorecard}/upload/dhis`, form, {
                headers: { cookie: headers.join() },
            });
        } catch (error) {
            console.log(error);
        } finally {
        }
    }
};

export const queryDHIS2 = async ({
    pe,
    scorecard,
    ou,
    includeChildren,
    id,
    status,
}: Schedule): Promise<void> => {
    if (status !== "running") {
        const jobId = crypto.randomUUID();
        const startTime = new Date().toISOString();
        let units: {
            organisationUnits: {
                id: string;
                name: string;
                level: number;
                organisationUnitGroups: { id: string }[];
            }[];
        } = { organisationUnits: [] };
        try {
            db.run(
                `INSERT INTO job_executions (id, scheduleId, startTime, status) VALUES (?, ?, ?, ?)`,
                [jobId, id, startTime, "running"],
            );

            db.run(
                `UPDATE schedules SET currentJobId = ?, status = 'running', progress = 0, message = 'Task started' WHERE id = ?`,
                [jobId, id],
            );
            db.run(`UPDATE schedules SET message = ? WHERE id = ?`, [
                "Fetching organisation units",
                id,
            ]);
        } catch (error) {
            console.log(error);
        }

        if (includeChildren) {
            const { data } = await dhis2Api.get(
                `organisationUnits/${ou}.json`,
                {
                    params: {
                        fields: "id,name,level,organisationUnitGroups",
                        includeDescendants: true,
                        paging: false,
                    },
                },
            );
            if (data && data.id) {
                units.organisationUnits = [data];
            } else {
                units = data;
            }
        } else {
            const { data } = await dhis2Api.get<{
                id: string;
                name: string;
                organisationUnitGroups: { id: string }[];
                level: number;
            }>(`organisationUnits/${ou}.json`, {
                params: { fields: "id,name,level,organisationUnitGroups" },
            });
            units.organisationUnits = [data];
        }

        const {
            data: { indicators },
        } = await dhis2Api.get<{ indicators: { id: string }[] }>(
            `indicatorGroups/SWDeaw0RUyR.json`,
            {
                params: { fields: "indicators[id,name]" },
            },
        );

        const allIndicators = indicators.map(({ id }) => id);
        let i = 1;
        const totalSteps = units.organisationUnits.length;
        for (const {
            id,
            name,
            organisationUnitGroups,
            level,
        } of units.organisationUnits) {
            const isNot4Profit =
                organisationUnitGroups.filter(
                    (a) => NOT_FOR_PROFIT.indexOf(a.id) !== -1,
                ).length > 0;
            let availableIndicators = allIndicators;
            if (!isNot4Profit && level === 5) {
                availableIndicators = availableIndicators.filter(
                    (i) => NOT_FOR_PROFIT_INDICATIONS.indexOf(i) === -1,
                );
            }
            const url = `analytics.json?dimension=dx:${availableIndicators.join(
                ";",
            )}&dimension=pe:${pe}&dimension=ou:${id}`;
            try {
                const { data } = await dhis2Api.get(url);
                await sendToAlma({
                    data,
                    scorecard,
                    name,
                });
            } catch (error) {
                for (const { id: indicator } of indicators) {
                    const url = `analytics.json?dimension=dx:${indicator}&dimension=pe:${pe}&dimension=ou:${id}`;
                    try {
                        const { data: data2 } = await dhis2Api.get(url);
                        await sendToAlma({
                            data: data2,
                            scorecard,
                            name,
                        });
                    } catch (error) {
                        console.log(error);
                    }
                }
            }
            const progress = Math.round((i / totalSteps) * 100);
            try {
                db.run(
                    `UPDATE schedules SET progress = ?, message = ? WHERE id = ?`,
                    [
                        progress,
                        `Completed step ${i} of ${totalSteps} (${name})`,
                        id,
                    ],
                );
            } catch (error) {
                console.log(error);
            }
            i = i + 1;
        }
        const endTime = new Date().toISOString();
        try {
            db.run(
                `UPDATE job_executions SET endTime = ?, status = ? WHERE id = ?`,
                [endTime, "completed", jobId],
            );

            db.run(
                `UPDATE schedules SET currentJobId = NULL, lastStatus = 'completed', status = 'completed', progress = 100, lastRun = ?, message = 'Task completed successfully' WHERE id = ?`,
                [endTime, id],
            );
        } catch (error) {
            console.log(error);
        }
    }
};
