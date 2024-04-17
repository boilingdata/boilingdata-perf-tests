import { BoilingData, isDataResponse } from "@boilingdata/node-boilingdata";
import { CloudWatchClient, PutMetricDataCommand } from "@aws-sdk/client-cloudwatch"; // ES Modules import
import fetch from "node-fetch";
import util from "node:util";

const sleep = (seconds) => new Promise((resolve) => setTimeout(resolve, seconds * 1000));

const bdInstance = new BoilingData({
  username: process.env["BD_USERNAME"],
  password: process.env["BD_PASSWORD"],
  logLevel: "info",
});

async function postMetricToNyrkio({ metricName, timeMs, succeeded }) {
  /*
    curl -s -X POST -H "Content-type: application/json" -H "Authorization: Bearer $TOKEN" https://nyrkio.com/api/v0/result/benchmark1 \
              -d '[{"timestamp": 1706220908,
                "metrics": [
                  {"name": "p50", "unit": "us", "value": 56 },
                  {"name": "p90", "unit": "us", "value": 125 },
                  {"name": "p99", "unit": "us", "value": 280 }
                ],
                "attributes": {
                  "git_repo": "https://github.com/nyrkio/nyrkio",
                  "branch": "main",
                  "git_commit": "6995e2de6891c724bfeb2db33d7b87775f913ad1",
                }
          }]'
  */
  const headers = {
    "Content-Type": "application/json",
    Authorization:
      "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiI2NjFmYTliMjQ0ZTg0NTBhNmI2MjI0OGIiLCJhdWQiOlsiZmFzdGFwaS11c2VyczphdXRoIl19.iLvU6tCXEwP8w8ev2Oz3NPOTOhW_ORr4nTSA2zXrfag",
  };
  const url = `https://nyrkio.com/api/v0/result/${metricName}`;
  const body = JSON.stringify([
    {
      timestamp: Math.round(Date.now() / 1000),
      metrics: [
        {
          name: metricName,
          unit: "ms",
          value: timeMs,
        },
      ],
      attributes: {
        git_repo: "https://github.com/boilingdata/boilingdata-perf-tests",
        branch: "main",
        git_commit: "f980ac90bfefea341c505a69623b756bd0f5f7d6",
      },
    },
  ]);
  const res = await fetch(url, { method: "POST", headers, body });
  const nyrkioResponse = await res.json();
  console.log(util.inspect({ nyrkioResponse }, false, 20, false));
}

async function runQuery(sql) {
  const rows = await new Promise((resolve, reject) => {
    let r = [];
    bdInstance.execQuery({
      sql,
      callbacks: {
        onData: (data) => {
          if (isDataResponse(data)) data.data.map((row) => r.push(row));
        },
        onQueryFinished: () => resolve(r),
        onLogError: (data) => reject(data),
      },
    });
  });
  return rows;
}

async function runTestQuery(cw, sql, MetricName) {
  try {
    const startTime = Date.now();
    const res = await runQuery(sql);
    const endTime = Date.now();
    const timeMs = endTime - startTime;
    const succeeded = res.length == 10;
    const metric = { metricName: MetricName, timeMs, succeeded };
    console.log(metric);
    if (succeeded) await postMetricToNyrkio(metric).catch(); // don't fail if this fails..
    const input = {
      Namespace: "boilingdata",
      MetricData: [
        {
          MetricName: "queryPerformance",
          Dimensions: [
            {
              Name: "QueryName",
              Value: MetricName,
            },
          ],
          Timestamp: new Date(),
          Value: timeMs,
          Unit: "Milliseconds",
          StorageResolution: Number(1),
        },
      ],
    };

    const command = new PutMetricDataCommand(input);
    const response = await cw.send(command);
    console.log({ httpStatusCode: response["$metadata"].httpStatusCode });
  } catch (err) {
    console.error(err);
  }
}

async function main() {
  let round = 1;
  // We run every hour with new BD connection
  while (true) {
    try {
      // connect and warm up boiling default router
      const cw = new CloudWatchClient({ region: "eu-west-1" });
      console.log("==== connecting");
      await bdInstance.connect();
      await runQuery("SELECT * FROM boilingdata;");
      let sql = "";

      console.log("==== Round:", round++);

      // ROUND:
      // 5 + 5 + 50 = 1min
      // 5x: 5 + 5 + 50 = 5min
      // Sleep 10min
      // TOTAL TIME: >15min
      // ==> WARMUP_TIME 17min?

      // Cold starts: demo, taxi
      sql = `SELECT * FROM parquet_scan('s3://boilingdata-demo/demo.parquet') LIMIT 10;`;
      await runTestQuery(cw, sql, "cold_demo");
      await sleep(5);

      sql = `SELECT * FROM parquet_scan('s3://boilingdata-demo/taxi_locations.parquet') LIMIT 10;`;
      await runTestQuery(cw, sql, "cold_taxi");
      await sleep(5);

      sql = `SELECT * FROM parquet_scan('s3://boilingdata-demo/demo2.parquet') LIMIT 10;`;
      await runTestQuery(cw, sql, "cold_demo");
      await sleep(50);

      // Warm but not in results cache, 4 rounds 1min apart
      for (let index = 1; index < 5; index++) {
        sql = `SELECT * FROM parquet_scan('s3://boilingdata-demo/demo.parquet') LIMIT ${10 + index};`;
        await runTestQuery(cw, sql, "warm_demo");
        await sleep(5);

        sql = `SELECT * FROM parquet_scan('s3://boilingdata-demo/taxi_locations.parquet') LIMIT ${10 + index};`;
        await runTestQuery(cw, sql, "warm_taxi");
        await sleep(5);

        sql = `SELECT * FROM parquet_scan('s3://boilingdata-demo/demo2.parquet') LIMIT ${10 + index};`;
        await runTestQuery(cw, sql, "warm_demo");
        await sleep(50);
      }

      // disconnect
      console.log("==== disconnecting");
      await bdInstance.close();
    } catch (err) {
      console.error(err);
    }
    console.log("==== sleeping 10 mins");
    await sleep(10 * 60); // 10 mins
  }
}

main();
