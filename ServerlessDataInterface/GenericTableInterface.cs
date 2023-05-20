using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Azure.Data.Tables;
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
        public static async Task<IActionResult> HandleRequestAsync(HttpRequest req, TableClient cloudTable, string id, string partitionKey, Dictionary<string, Dictionary<string, TypeHints>> typeHints, IAccessController accessController = null)
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
                TableEntity dynamicEntity = ConvertToTableEntity(id, partitionKey, data);
                cloudTable.UpsertEntity(dynamicEntity);
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

                TableEntity dynamicEntity = ConvertToTableEntity(id, partitionKey, data);
                cloudTable.UpdateEntity(dynamicEntity, new Azure.ETag("*"));
                return new OkResult();
            }
            else if (req.Method == "GET")
            {
                if (id != null)
                {
                    var result = cloudTable.GetEntity<TableEntity>(partitionKey, id);
                    var getOutput = ConvertFromTableEntity(result, currentTableHints);
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
                        filters.Add(BuildQueryComponent(queryItemKey[..^3], "ne", queryItem[0], currentTableHints));
                        continue;
                    }
                    if (queryItemKey.EndsWith("_gte"))
                    {
                        filters.Add(BuildQueryComponent(queryItemKey[..^4], "ge", queryItem[0], currentTableHints));
                        continue;
                    }
                    if (queryItemKey.EndsWith("_lte"))
                    {
                        filters.Add(BuildQueryComponent(queryItemKey[..^4], "le", queryItem[0], currentTableHints));
                        continue;
                    }
                    if (queryItemKey == "id")
                    {
                        var tmpFilter = string.Empty;
                        foreach (var item in queryItem)
                        {
                            if (tmpFilter == string.Empty)
                            {
                                tmpFilter =  $"RowKey eq '{item}'";
                            }
                            else
                            {
                                tmpFilter = $"{tmpFilter} or RowKey eq '${item}'";
                            }
                        }
                        filters.Add(tmpFilter);
                        continue;
                    }
                    filters.Add(BuildQueryComponent(queryItemKey, "eq", queryItem[0], currentTableHints));
                }
                var compiledQuery = string.Join(" and ", filters);
                var multipleResult = filters.Count == 0 ? cloudTable.Query<TableEntity>() : cloudTable.Query<TableEntity>(compiledQuery);
                foreach (var item in multipleResult)
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
                var retrieveResult = cloudTable.GetEntity<TableEntity>(partitionKey, id);
                if(retrieveResult == null)
                {
                    return new NotFoundResult();
                }
                var record = ConvertFromTableEntity(retrieveResult, currentTableHints);
                if (accessController != null)
                {
                    var accessControl = accessController.CheckAccess(cloudTable.Name, AccessType.Read, id);
                    if (!accessControl.Allowed)
                    {
                        return new UnauthorizedResult();
                    }
                }
                cloudTable.DeleteEntity(partitionKey, id);
                return new OkResult();
            }

            return new OkObjectResult("");
        }
        private static string BuildQueryComponent(string fieldName, string comparison, string value, Dictionary<string, TypeHints> typeHints)
        {
            if (!typeHints.ContainsKey(fieldName))
            {
                return $"{fieldName} {comparison} '{value}'";
            }

            if (typeHints[fieldName] == TypeHints.Boolean && bool.TryParse(value, out bool parsedBool))
            {
                return $"{fieldName} {comparison} {(parsedBool ? "true" : "false")}";
            }
            if (typeHints[fieldName] == TypeHints.Date && DateTime.TryParse(value, out DateTime parsedDate))
            {
                return $"{fieldName} {comparison} {parsedDate}";
            }
            if (typeHints[fieldName] == TypeHints.Integer && int.TryParse(value, out int parsedInt))
            {
                return $"{fieldName} {comparison} {parsedInt}";
            }

            return $"{fieldName} {comparison} '{value}'";
;
        }

        private static TableEntity ConvertToTableEntity(string id, string partitionKey, Dictionary<string, object> data)
        {
            var dynamicEntity = new TableEntity(partitionKey, !string.IsNullOrEmpty(id) ? id : Guid.NewGuid().ToString());
            foreach (var (key, value) in data)
            {
                switch (value)
                {
                    case string convertedValue:
                        dynamicEntity[key] = convertedValue;
                        break;
                    case bool convertedValue:
                        dynamicEntity[key] = convertedValue;
                        break;
                    case int convertedValue:
                        dynamicEntity[key] = convertedValue;
                        break;
                    case float convertedValue:
                        dynamicEntity[key] = convertedValue;
                        break;
                    case double convertedValue:
                        dynamicEntity[key] = convertedValue;
                        break;
                    case long convertedValue:
                        dynamicEntity[key] = convertedValue;
                        break;
                    case DateTime convertedValue:
                        dynamicEntity[key] = convertedValue;
                        break;
                    case JArray convertedValue:
                        dynamicEntity[key] = string.Join(",", convertedValue.Select(i => i.ToString()));
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

        private static Dictionary<string, object> ConvertFromTableEntity(TableEntity item, Dictionary<string, TypeHints> typeHints)
        {
            var output = new Dictionary<string, object>();
            foreach (var (key, field) in item)
            {
                switch (field)
                {
                    case string _:
                        if (typeHints.ContainsKey(key) && typeHints[key] == TypeHints.ListString)
                        {
                            output[key] = ((string)field).Split(",").ToList();
                            break;
                        }
                        output.Add(key, (string)field);
                        break;
                    default:
                        output.Add(key, field);
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
