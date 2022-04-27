using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Cosmos.Table;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ServerlessDataInterface
{
    public static class GenericTableInterface
    {
        public static async Task<IActionResult> HandleRequestAsync(HttpRequest req, CloudTable cloudTable, string id, string partitionKey, Dictionary<string, Dictionary<string, TypeHints>> typeHints, IAccessController accessController = null)
        {
            var currentTableHints = typeHints.ContainsKey(cloudTable.Name) ? typeHints[cloudTable.Name] : new Dictionary<string, TypeHints>();

            if (req.Method == "POST")
            {
                string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
                var data = JsonConvert.DeserializeObject<Dictionary<string, object>>(requestBody);
                if (accessController != null)
                {
                    var accessControl = accessController.CheckAccess(cloudTable.Name, AccessType.Create, id, data);
                    if (!accessControl.Allowed)
                    {
                        return new UnauthorizedResult();
                    }
                }
                DynamicTableEntity dynamicEntity = ConvertToTableEntity(id, partitionKey, data);
                var insertOperation = TableOperation.InsertOrReplace(dynamicEntity);
                cloudTable.Execute(insertOperation);
                return new OkResult();
            }
            else if (req.Method == "PATCH")
            {
                string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
                var data = JsonConvert.DeserializeObject<Dictionary<string, object>>(requestBody);

                if (accessController != null)
                {
                    var accessControl = accessController.CheckAccess(cloudTable.Name, AccessType.Write, id, data);
                    if (!accessControl.Allowed)
                    {
                        return new UnauthorizedResult();
                    }
                    if (!accessControl.AllFieldsAllowed)
                    {
                        data = RemoveAllButListedFields(data, accessControl.AllowedFields);
                    }
                }

                DynamicTableEntity dynamicEntity = ConvertToTableEntity(id, partitionKey, data);
                dynamicEntity.ETag = "*";
                var insertOperation = TableOperation.Merge(dynamicEntity);
                cloudTable.Execute(insertOperation);
                return new OkResult();
            }
            else if (req.Method == "GET")
            {
                if (id != null)
                {
                    var retrieveCommand = TableOperation.Retrieve<DynamicTableEntity>(partitionKey, id);
                    var result = cloudTable.Execute(retrieveCommand);
                    var getOutput = ConvertFromTableEntity((DynamicTableEntity)result.Result, currentTableHints);
                    if (accessController != null)
                    {
                        var accessControl = accessController.CheckAccess(cloudTable.Name, AccessType.Read, id, getOutput);
                        if (!accessControl.Allowed)
                        {
                            return new UnauthorizedResult();
                        }
                        if (!accessControl.AllFieldsAllowed)
                        {
                            getOutput = RemoveAllButListedFields(getOutput, accessControl.AllowedFields);
                        }
                    }
                    return new OkObjectResult(getOutput);
                }
                // Get all

                // Add total count so that the application knows what the total amount is
                req.HttpContext.Response.Headers.Add("Access-Control-Expose-Headers", "x-total-count");
                var output = new List<Dictionary<string, object>>();
                var query = new TableQuery<DynamicTableEntity>();
                string filter = string.Empty;
                var filters = new List<string>();
                foreach (var (queryItemKey, queryItem) in req.Query)
                {
                    if (queryItemKey.EndsWith("_like"))
                    {
                        continue;
                    }
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
                        foreach (var item in queryItem)
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
                    output.Add(ConvertFromTableEntity(item, currentTableHints));
                }

                // Do filtering that the db can't handle
                foreach (var (queryItemKey, queryItem) in req.Query)
                {
                    if (queryItemKey.EndsWith("_like"))
                    {
                        output = output.Where(i => i[queryItemKey[..^5].ToString()].ToString()?.Contains(queryItem[0], StringComparison.OrdinalIgnoreCase) ?? false).ToList();
                    }
                }

                // Clean out allowed records
                if (accessController != null)
                {
                    output = CleanAuthorizedRecords(cloudTable.Name, output, accessController);
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
                if (req.Query.ContainsKey("_start") && int.TryParse(req.Query["_start"][0], out int start) && start >= 0)
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
                var retrieveCommand = TableOperation.Retrieve(partitionKey, id);
                var retrieveResult = cloudTable.Execute(retrieveCommand);
                if(retrieveResult.HttpStatusCode == 404)
                {
                    return new NotFoundResult();
                }
                var record = ConvertFromTableEntity((DynamicTableEntity)retrieveResult.Result, currentTableHints);
                if (accessController != null)
                {
                    var accessControl = accessController.CheckAccess(cloudTable.Name, AccessType.Read, id);
                    if (!accessControl.Allowed)
                    {
                        return new UnauthorizedResult();
                    }
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

            if (typeHints[fieldName] == TypeHints.Boolean && bool.TryParse(value, out bool parsedBool))
            {
                return TableQuery.GenerateFilterConditionForBool(fieldName, comparison, parsedBool);
            }
            if (typeHints[fieldName] == TypeHints.Date && DateTime.TryParse(value, out DateTime parsedDate))
            {
                return TableQuery.GenerateFilterConditionForDate(fieldName, comparison, parsedDate);
            }
            if (typeHints[fieldName] == TypeHints.Integer && int.TryParse(value, out int parsedInt))
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
                    case long convertedValue:
                        dynamicEntity[key] = new EntityProperty(convertedValue);
                        break;
                    case DateTime convertedValue:
                        dynamicEntity[key] = new EntityProperty(convertedValue);
                        break;
                    case JArray convertedValue:
                        dynamicEntity[key] = new EntityProperty(string.Join(",", convertedValue.Select(i => i.ToString())));
                        break;
                }
            }

            return dynamicEntity;
        }
        private static Dictionary<string,object> RemoveAllButListedFields(Dictionary<string,object> data, List<string> allowedFields)
        {
            var output = new Dictionary<string,object>(data.Count);
            foreach(var (key,value) in data)
            {
                if (allowedFields.Contains(key))
                {
                    output.Add(key, value);
                }
            }
            return output;
        }
        private static List<Dictionary<string,object>> CleanAuthorizedRecords(string tableName, List<Dictionary<string,object>> input, IAccessController accessController)
        {
            var output = new List<Dictionary<string,object>>(input.Count);
            foreach(var row in input)
            {
                var access = accessController.CheckAccess(tableName, AccessType.Read, (string)row["id"], row);
                if (!access.Allowed)
                {
                    continue;
                }
                if (access.AllFieldsAllowed)
                {
                    output.Add(row);
                    continue;
                }
                output.Add(RemoveAllButListedFields(row, access.AllowedFields));
            }
            return output;
        }

        private static Dictionary<string, object> ConvertFromTableEntity(DynamicTableEntity item, Dictionary<string, TypeHints> typeHints)
        {
            var output = new Dictionary<string, object>();
            foreach (var (key, field) in item.Properties)
            {
                switch (field.PropertyType)
                {
                    case EdmType.String:
                        if (typeHints.ContainsKey(key) && typeHints[key] == TypeHints.ListString)
                        {
                            output[key] = field.StringValue.Split(",").ToList();
                            break;
                        }
                        output.Add(key, field.StringValue);
                        break;
                    case EdmType.Boolean:
                        output.Add(key, field.BooleanValue);
                        break;
                    case EdmType.Int32:
                        output.Add(key, field.Int32Value);
                        break;
                    case EdmType.Int64:
                        output.Add(key, field.Int64Value);
                        break;
                    case EdmType.DateTime:
                        output.Add(key, field.DateTime);
                        break;
                    case EdmType.Double:
                        output.Add(key, field.DoubleValue);
                        break;
                }
            }
            output.Add("id", item.RowKey);
            return output;
        }
    }
    public enum TypeHints
    {
        Boolean,
        Date,
        Integer,
        ListString,
    }
}
