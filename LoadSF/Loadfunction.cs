using System.Data;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Snowflake.Data.Client;
using System;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;

namespace LoadSF
{
    public static class Loadfunction
    {
        [FunctionName("Loadfunction")]
        public static async Task<IActionResult> Run([HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req, ILogger log) 
        {
            log.LogInformation("C# HTTP trigger function processed a COPY request.");

            string host = req.Query["host"];
            string account = req.Query["account"];
            string user = req.Query["user"];
            string secretname = req.Query["secretname"];
            string database = req.Query["database"];
            string warehouse = req.Query["warehouse"];
            string schema = req.Query["schema"];
            string table = req.Query["table"];
            string stage = req.Query["stage"];

            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            dynamic data = JsonConvert.DeserializeObject(requestBody);

            host = host ?? data?.host;
            account = account ?? data?.account;
            user = user ?? data?.user;
            secretname = secretname ?? data?.secretname;
            database = database ?? data?.database;
            warehouse = warehouse ?? data?.warehouse;
            schema = schema ?? data?.schema;
            table = table ?? data?.table;
            stage = stage ?? data?.stage;

            log.LogInformation($"Requesting setting {secretname}.");
            string pwd = Environment.GetEnvironmentVariable(secretname);

            using (IDbConnection conn = new SnowflakeDbConnection())
            {              
                conn.ConnectionString = $"host={host};account={account};user={user};password={pwd};db={database};schema={schema};warehouse={warehouse}";
                conn.Open();

                int affectedrows = 0;

                IDbCommand cmd = conn.CreateCommand();
                cmd.CommandText = $"copy into {table} from '@{stage}'";
                IDataReader reader = cmd.ExecuteReader();
                affectedrows = cmd.ExecuteNonQuery();

                conn.Close();

                log.LogInformation($"Processed COPY for {warehouse} {database} {table}");

                return ReturnMethod(affectedrows);
            }
        }

        private static ActionResult ReturnMethod(int affectedrows)
         {
             return affectedrows < 1
                     ? (ActionResult)new OkObjectResult(new { Result = "OK" })
                     : new BadRequestObjectResult(new { Result = "Failed" });
         }
    }
}