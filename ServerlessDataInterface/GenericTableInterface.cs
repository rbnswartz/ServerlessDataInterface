using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Cosmos.Table;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TestTableStorageOutput
{
    public static class GenericTableInterface
    { 
        public static async Task<IActionResult> HandleRequestAsync(HttpRequest req, CloudTable cloudTable, string id, string partitionKey, Dictionary<string,Dictionary<string,TypeHints>> typeHints)
        {
            var currentTableHints = typeHints.ContainsKey(cloudTable.Name) ? typeHints[cloudTable.Name] : new Dictionary<string, TypeHints>();

            if (req.Method == "POST")
            {
                string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
                var data = JsonConvert.DeserializeObject<Dictionary<string, object>>(requestBody);
                DynamicTableEntity dynamicEntity = ConvertToTableEntity(id, partitionKey, data);
                var insertOperation = TableOperation.InsertOrReplace(dynamicEntity);
                cloudTable.Execute(insertOperation);
                return new OkResult();
            }
            else if (req.Method == "PATCH")
            {
                string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
                var data = JsonConvert.DeserializeObject<Dictionary<string, object>>(requestBody);
                DynamicTableEntity dynamicEntity = ConvertToTableEntity(id, partitionKey, data);
                dynamicEntity.ETag = "*";
                var insertOperation = TableOperation.Replace(dynamicEntity);
                cloudTable.Execute(insertOperation);
                return new OkResult();
            }
            else if (req.Method == "GET")
            {
                if (id != null)
                {
                    var retrieveCommand = TableOperation.Retrieve<DynamicTableEntity>(partitionKey, id);
                    var result = cloudTable.Execute(retrieveCommand);
                    return new OkObjectResult(ConvertFromTableEntity((DynamicTableEntity)result.Result));
                }
                // Get all

                // Add total count so that the application knows what the total amount is
                req.HttpContext.Response.Headers.Add("Access-Control-Expose-Headers", "x-total-count");
                var output = new List<Dictionary<string, object>>();
                var query = new TableQuery<DynamicTableEntity>();
                string filter = string.Empty;
                var filters = new List<string>();
                foreach(var (queryItemKey, queryItem) in req.Query)
                {
                    if (queryItemKey.StartsWith("_"))
                    {
                        continue;
                    }
                    if (queryItemKey.EndsWith("_ne"))
                    {
                        filters.Add(BuildQueryComponent(queryItemKey[..^3], QueryComparisons.NotEqual, queryItem[0], currentTableHints));
                        continue;
                    }
                    if (queryItemKey.EndsWith("_gte"))
                    {
                        filters.Add(BuildQueryComponent(queryItemKey[..^4], QueryComparisons.GreaterThanOrEqual, queryItem[0], currentTableHints));
                        continue;
                    }
                    if (queryItemKey.EndsWith("_lte"))
                    {
                        filters.Add(BuildQueryComponent(queryItemKey[..^4], QueryComparisons.LessThanOrEqual, queryItem[0], currentTableHints));
                        continue;
                    }
                    if (queryItemKey == "id")
                    {
                        var tmpFilter = string.Empty;
                        foreach(var item in queryItem) 
                        {
                            if (tmpFilter == string.Empty)
                            {
                                tmpFilter = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, item);
                            }
                            else
                            {
                                tmpFilter = TableQuery.CombineFilters(tmpFilter, TableOperators.Or, TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, item));
                            }
                        }
                        filters.Add(tmpFilter);
                        continue;
                    }
                    filters.Add(BuildQueryComponent(queryItemKey, QueryComparisons.Equal, queryItem[0], currentTableHints));
                }
                query.Where(string.Join(" and ", filters));
                foreach (var item in cloudTable.ExecuteQuery(query))
                {
                    output.Add(ConvertFromTableEntity(item));
                }

                // Set total count
                req.HttpContext.Response.Headers.Add("x-total-count", output.Count.ToString());

                // Sorting isn't supported in Azure storage so we're just going to do our own here
                if (req.Query.ContainsKey("_sort"))
                {
                    var orderKey = req.Query["_sort"];
                    var descending = req.Query.ContainsKey("_order") && req.Query["_order"] == "desc";
                    if (descending)
                    {
                        output = output.OrderByDescending(i => i.ContainsKey(orderKey) ? i[orderKey] : null).ToList();
                    }
                    else
                    {
                        output = output.OrderBy(i => i.ContainsKey(orderKey) ? i[orderKey] : null).ToList();
                    }
                }

                // Do start and end offset
                if (req.Query.ContainsKey("_start") && int.TryParse(req.Query["_start"][0], out int start) && start >=0)
                {
                    if (start > output.Count)
                    {
                        return new OkObjectResult(new List<Dictionary<string, object>>());
                    }
                    if (req.Query.ContainsKey("_end") && int.TryParse(req.Query["_end"][0], out int end) && end >= 0)
                    {
                        if (start > end)
                        {
                            return new BadRequestResult();
                        }
                        if (end > output.Count)
                        {
                            end = output.Count;
                        }
                        return new OkObjectResult(output.ToArray()[start..end]);
                    }
                }

                return new OkObjectResult(output);
            }
            else if (req.Method == "DELETE")
            {
                if (string.IsNullOrEmpty(id))
                {
                    return new BadRequestResult();
                }
                var deleteCommand = TableOperation.Delete(new DynamicTableEntity(partitionKey, id) { ETag = "*" });
                cloudTable.Execute(deleteCommand);
                return new OkResult();
            }

            return new OkObjectResult("");
        }
        private static string BuildQueryComponent(string fieldName, string comparison, string value, Dictionary<string, TypeHints> typeHints)
        {
            if (!typeHints.ContainsKey(fieldName))
            {
                return TableQuery.GenerateFilterCondition(fieldName, comparison, value);
            }

            if(typeHints[fieldName] == TypeHints.Boolean && bool.TryParse(value, out bool parsedBool))
            {
                return TableQuery.GenerateFilterConditionForBool(fieldName, comparison, parsedBool);
            }
            if (typeHints[fieldName] == TypeHints.Date && DateTime.TryParse( value, out DateTime parsedDate))
            {
                return TableQuery.GenerateFilterConditionForDate(fieldName, comparison, parsedDate);
            }
            if (typeHints[fieldName] == TypeHints.Integer && int.TryParse( value, out int parsedInt))
            {
                return TableQuery.GenerateFilterConditionForInt(fieldName, comparison, parsedInt);
            }

            return TableQuery.GenerateFilterCondition(fieldName, comparison, value);
        }
        
        private static DynamicTableEntity ConvertToTableEntity(string id, string partitionKey, Dictionary<string, object> data)
        {
            DynamicTableEntity dynamicEntity = new DynamicTableEntity(partitionKey, !string.IsNullOrEmpty(id) ? id : Guid.NewGuid().ToString());
            foreach (var (key, value) in data)
            {
                switch (value)
                {
                    case string convertedValue:
                        dynamicEntity[key] = new EntityProperty(convertedValue);
                        break;
                    case bool convertedValue:
                        dynamicEntity[key] = new EntityProperty(convertedValue);
                        break;
                    case int convertedValue:
                        dynamicEntity[key] = new EntityProperty(convertedValue);
                        break;
                    case float convertedValue:
                        dynamicEntity[key] = new EntityProperty(convertedValue);
                        break;
                    case double convertedValue:
                        dynamicEntity[key] = new EntityProperty(convertedValue);
                        break;
                    case DateTime convertedValue:
                        dynamicEntity[key] = new EntityProperty(convertedValue);
                        break;

                }
            }

            return dynamicEntity;
        }

        private static Dictionary<string, object> ConvertFromTableEntity(DynamicTableEntity item)
        {
            var tmp = new Dictionary<string, object>();
            foreach (var (key, field) in item.Properties)
            {
                switch (field.PropertyType)
                {
                    case EdmType.String:
                        tmp.Add(key, field.StringValue);
                        break;
                    case EdmType.Boolean:
                        tmp.Add(key, field.BooleanValue);
                        break;
                    case EdmType.Int32:
                        tmp.Add(key, field.Int32Value);
                        break;
                    case EdmType.DateTime:
                        tmp.Add(key, field.DateTime);
                        break;
                    case EdmType.Double:
                        tmp.Add(key, field.DoubleValue);
                        break;

                }
            }
            tmp.Add("id", item.RowKey);
            return tmp;
        }
    }
    public enum TypeHints
    {
        Boolean,
        Date,
        Integer,
    }
}
