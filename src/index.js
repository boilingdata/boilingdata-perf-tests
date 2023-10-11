import { BoilingData, isDataResponse } from "@boilingdata/node-boilingdata";
import { CloudWatchClient, PutMetricDataCommand } from "@aws-sdk/client-cloudwatch"; // ES Modules import

const sleep = (seconds) => new Promise((resolve) => setTimeout(resolve, seconds * 1000));

const bdInstance = new BoilingData({
  username: process.env["BD_USERNAME"],
  password: process.env["BD_PASSWORD"],
  logLevel: "info",
});

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
    const metric = { metricName: MetricName, timeMs, succeeded: res.length == 10 };
    console.log(metric);
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

      console.log("==== Round:", round);

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
